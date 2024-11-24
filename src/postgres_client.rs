#![allow(clippy::arithmetic_side_effects)]

mod postgres_client_transaction;

/// A concurrent implementation for writing accounts into the PostgreSQL in parallel.
use {
    crate::{
        geyser_plugin_postgres::{GeyserPluginPostgresConfig, GeyserPluginPostgresError},
    },
    crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender},
    log::*,
    postgres::{Client, NoTls, Statement},
    postgres_client_transaction::LogTransactionRequest,
    agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError,
    solana_measure::measure::Measure,
    solana_metrics::*,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        thread::{self, sleep, Builder, JoinHandle},
        time::Duration,
    },
};

/// The maximum asynchronous requests allowed in the channel to avoid excessive
/// memory usage. The downside -- calls after this threshold is reached can get blocked.
const MAX_ASYNC_REQUESTS: usize = 40960;
const SAFE_BATCH_STARTING_SLOT_CUSHION: u64 = 2 * 40960;
const DEFAULT_POSTGRES_PORT: u16 = 5432;
const DEFAULT_THREADS_COUNT: usize = 100;
const DEFAULT_PANIC_ON_DB_ERROR: bool = false;

struct PostgresSqlClientWrapper {
    client: Client,
    update_transaction_log_stmt: Statement,
}

pub struct SimplePostgresClient {
    client: Mutex<PostgresSqlClientWrapper>,
}

struct PostgresClientWorker {
    client: SimplePostgresClient,
    /// Indicating if accounts notification during startup is done.
    is_startup_done: bool,
}

impl Eq for DbAccountInfo {}

#[derive(Clone, PartialEq, Debug)]
pub struct DbAccountInfo {
    pub pubkey: Vec<u8>,
    pub lamports: i64,
    pub owner: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: i64,
    pub data: Vec<u8>,
    pub slot: i64,
    pub write_version: i64,
    pub txn_signature: Option<Vec<u8>>,
}

pub(crate) fn abort() -> ! {
    #[cfg(not(test))]
    {
        // standard error is usually redirected to a log file, cry for help on standard output as
        // well
        eprintln!("Validator process aborted. The validator log may contain further details");
        std::process::exit(1);
    }

    #[cfg(test)]
    panic!("process::exit(1) is intercepted for friendly test failure...");
}

pub trait PostgresClient {
    fn join(&mut self) -> thread::Result<()> {
        Ok(())
    }

    fn log_transaction(
        &mut self,
        transaction_log_info: LogTransactionRequest,
    ) -> Result<(), GeyserPluginError>;

}

impl SimplePostgresClient {
    pub fn connect_to_db(config: &GeyserPluginPostgresConfig) -> Result<Client, GeyserPluginError> {
        let port = config.port.unwrap_or(DEFAULT_POSTGRES_PORT);

        let connection_str = if let Some(connection_str) = &config.connection_str {
            connection_str.clone()
        } else {
            if config.host.is_none() || config.user.is_none() {
                let msg = format!(
                    "\"connection_str\": {:?}, or \"host\": {:?} \"user\": {:?} must be specified",
                    config.connection_str, config.host, config.user
                );
                return Err(GeyserPluginError::Custom(Box::new(
                    GeyserPluginPostgresError::ConfigurationError { msg },
                )));
            }
            format!(
                "host={} user={} port={}",
                config.host.as_ref().unwrap(),
                config.user.as_ref().unwrap(),
                port
            )
        };

        let result = Client::connect(&connection_str, NoTls);

        match result {
            Err(err) => {
                let msg = format!(
                    "Error in connecting to the PostgreSQL database: {:?} connection_str: {:?}",
                    err, connection_str
                );
                error!("{}", msg);
                Err(GeyserPluginError::Custom(Box::new(
                    GeyserPluginPostgresError::DataStoreConnectionError { msg },
                )))
            }
            Ok(client) => Ok(client),
        }
    }

    pub fn new(config: &GeyserPluginPostgresConfig) -> Result<Self, GeyserPluginError> {
        info!("Creating SimplePostgresClient...");
        let mut client = Self::connect_to_db(config)?;
        let update_transaction_log_stmt =
            Self::build_transaction_info_upsert_statement(&mut client, config)?;

        info!("Created SimplePostgresClient.");
        Ok(Self {
            client: Mutex::new(PostgresSqlClientWrapper {
                client,
                update_transaction_log_stmt,
            }),
        })
    }

    fn get_highest_available_slot(&mut self) -> Result<u64, GeyserPluginError> {
        let client = self.client.get_mut().unwrap();

        let last_slot_query = "SELECT slot FROM slot ORDER BY slot DESC LIMIT 1;";

        let result = client.client.query_opt(last_slot_query, &[]);
        match result {
            Ok(opt_slot) => Ok(opt_slot
                .map(|row| {
                    let raw_slot: i64 = row.get(0);
                    raw_slot as u64
                })
                .unwrap_or(0)),
            Err(err) => {
                let msg = format!(
                    "Failed to receive last slot from PostgreSQL database. Error: {:?}",
                    err
                );
                error!("{}", msg);
                Err(GeyserPluginError::AccountsUpdateError { msg })
            }
        }
    }
}

impl PostgresClient for SimplePostgresClient {

    fn log_transaction(
        &mut self,
        transaction_log_info: LogTransactionRequest,
    ) -> Result<(), GeyserPluginError> {
        self.log_transaction_impl(transaction_log_info)
    }
}

#[warn(clippy::large_enum_variant)]
enum DbWorkItem {
    LogTransaction(Box<LogTransactionRequest>),
}

impl PostgresClientWorker {
    fn new(config: GeyserPluginPostgresConfig) -> Result<Self, GeyserPluginError> {
        let result = SimplePostgresClient::new(&config);
        match result {
            Ok(client) => Ok(PostgresClientWorker {
                client,
                is_startup_done: false,
            }),
            Err(err) => {
                error!("Error in creating SimplePostgresClient: {}", err);
                Err(err)
            }
        }
    }

    fn do_work(
        &mut self,
        receiver: Receiver<DbWorkItem>,
        exit_worker: Arc<AtomicBool>,
        is_startup_done: Arc<AtomicBool>,
        startup_done_count: Arc<AtomicUsize>,
        panic_on_db_errors: bool,
    ) -> Result<(), GeyserPluginError> {
        while !exit_worker.load(Ordering::Relaxed) {
            let mut measure = Measure::start("geyser-plugin-postgres-worker-recv");
            let work = receiver.recv_timeout(Duration::from_millis(500));
            measure.stop();
            inc_new_counter_debug!(
                "geyser-plugin-postgres-worker-recv-us",
                measure.as_us() as usize,
                100000,
                100000
            );
            match work {
                Ok(work) => match work {
                    DbWorkItem::LogTransaction(transaction_log_info) => {
                        if let Err(err) = self.client.log_transaction(*transaction_log_info) {
                            error!("Failed to update transaction: ({})", err);
                            if panic_on_db_errors {
                                abort();
                            }
                        }
                    }
                },
                Err(err) => match err {
                    RecvTimeoutError::Timeout => {
                        if !self.is_startup_done && is_startup_done.load(Ordering::Relaxed) {
                            self.is_startup_done = true;
                            startup_done_count.fetch_add(1, Ordering::Relaxed);
                        }

                        continue;
                    }
                    _ => {
                        error!("Error in receiving the item {:?}", err);
                        if panic_on_db_errors {
                            abort();
                        }
                        break;
                    }
                },
            }
        }
        Ok(())
    }
}
pub struct ParallelPostgresClient {
    workers: Vec<JoinHandle<Result<(), GeyserPluginError>>>,
    exit_worker: Arc<AtomicBool>,
    is_startup_done: Arc<AtomicBool>,
    startup_done_count: Arc<AtomicUsize>,
    initialized_worker_count: Arc<AtomicUsize>,
    sender: Sender<DbWorkItem>,
    transaction_write_version: AtomicU64,
}

impl ParallelPostgresClient {
    pub fn new(config: &GeyserPluginPostgresConfig) -> Result<Self, GeyserPluginError> {
        info!("Creating ParallelPostgresClient...");
        let (sender, receiver) = bounded(MAX_ASYNC_REQUESTS);
        let exit_worker = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::default();
        let is_startup_done = Arc::new(AtomicBool::new(false));
        let startup_done_count = Arc::new(AtomicUsize::new(0));
        let worker_count = config.threads.unwrap_or(DEFAULT_THREADS_COUNT);
        let initialized_worker_count = Arc::new(AtomicUsize::new(0));
        for i in 0..worker_count {
            let cloned_receiver = receiver.clone();
            let exit_clone = exit_worker.clone();
            let is_startup_done_clone = is_startup_done.clone();
            let startup_done_count_clone = startup_done_count.clone();
            let initialized_worker_count_clone = initialized_worker_count.clone();
            let config = config.clone();
            let worker = Builder::new()
                .name(format!("worker-{}", i))
                .spawn(move || -> Result<(), GeyserPluginError> {
                    let panic_on_db_errors = *config
                        .panic_on_db_errors
                        .as_ref()
                        .unwrap_or(&DEFAULT_PANIC_ON_DB_ERROR);
                    let result = PostgresClientWorker::new(config);

                    match result {
                        Ok(mut worker) => {
                            initialized_worker_count_clone.fetch_add(1, Ordering::Relaxed);
                            worker.do_work(
                                cloned_receiver,
                                exit_clone,
                                is_startup_done_clone,
                                startup_done_count_clone,
                                panic_on_db_errors,
                            )?;
                            Ok(())
                        }
                        Err(err) => {
                            error!("Error when making connection to database: ({})", err);
                            if panic_on_db_errors {
                                abort();
                            }
                            Err(err)
                        }
                    }
                })
                .unwrap();

            workers.push(worker);
        }

        info!("Created ParallelPostgresClient.");
        Ok(Self {
            workers,
            exit_worker,
            is_startup_done,
            startup_done_count,
            initialized_worker_count,
            sender,
            transaction_write_version: AtomicU64::default(),
        })
    }

    pub fn join(&mut self) -> thread::Result<()> {
        self.exit_worker.store(true, Ordering::Relaxed);
        while !self.workers.is_empty() {
            let worker = self.workers.pop();
            if worker.is_none() {
                break;
            }
            let worker = worker.unwrap();
            let result = worker.join().unwrap();
            if result.is_err() {
                error!("The worker thread has failed: {:?}", result);
            }
        }

        Ok(())
    }


    pub fn notify_end_of_startup(&self) -> Result<(), GeyserPluginError> {
        info!("Notifying the end of startup");
        // Ensure all items in the queue has been received by the workers
        while !self.sender.is_empty() {
            sleep(Duration::from_millis(100));
        }
        self.is_startup_done.store(true, Ordering::Relaxed);

        // Wait for all worker threads to be done with flushing
        while self.startup_done_count.load(Ordering::Relaxed)
            != self.initialized_worker_count.load(Ordering::Relaxed)
        {
            info!(
                "Startup done count: {}, good worker thread count: {}",
                self.startup_done_count.load(Ordering::Relaxed),
                self.initialized_worker_count.load(Ordering::Relaxed)
            );
            sleep(Duration::from_millis(100));
        }

        info!("Done with notifying the end of startup");
        Ok(())
    }
}

pub struct PostgresClientBuilder {}

impl PostgresClientBuilder {
    pub fn build_parallel_postgres_client(
        config: &GeyserPluginPostgresConfig,
    ) -> Result<(ParallelPostgresClient, Option<u64>), GeyserPluginError> {
        let batch_optimize_by_skipping_older_slots =
            match config.skip_upsert_existing_accounts_at_startup {
                true => {
                    let mut on_load_client = SimplePostgresClient::new(config)?;

                    // database if populated concurrently so we need to move some number of slots
                    // below highest available slot to make sure we do not skip anything that was already in DB.
                    let batch_slot_bound = on_load_client
                        .get_highest_available_slot()?
                        .saturating_sub(SAFE_BATCH_STARTING_SLOT_CUSHION);
                    info!(
                        "Set batch_optimize_by_skiping_older_slots to {}",
                        batch_slot_bound
                    );
                    Some(batch_slot_bound)
                }
                false => None,
            };

        ParallelPostgresClient::new(config).map(|v| (v, batch_optimize_by_skipping_older_slots))
    }
}
