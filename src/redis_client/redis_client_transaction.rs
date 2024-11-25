use redis::Commands;
/// Module responsible for handling persisting transaction data to the PostgreSQL
/// database.
use {
    crate::{
        redis_client::{DbWorkItem, ParallelPostgresClient, SimplePostgresClient},
    },
    log::*,
    serde_derive::Serialize,
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, ReplicaTransactionInfoV2,
    },
    solana_runtime::bank::RewardType,
    solana_sdk::{
        instruction::CompiledInstruction,
        message::{
            v0::{self, MessageAddressTableLookup},
            Message, MessageHeader, SanitizedMessage,
        },
        transaction::TransactionError,
    },
    solana_transaction_status::{
        InnerInstructions, Reward, TransactionStatusMeta, TransactionTokenBalance,
    },
    std::sync::atomic::Ordering,
};

const MAX_TRANSACTION_STATUS_LEN: usize = 256;

#[derive(Clone, Debug, Serialize)]
pub struct DbCompiledInstruction {
    pub program_id_index: i16,
    pub accounts: Vec<i16>,
    pub data: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbInnerInstructions {
    pub index: i16,
    pub instructions: Vec<DbCompiledInstruction>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbTransactionTokenBalance {
    pub account_index: i16,
    pub mint: String,
    pub ui_token_amount: Option<f64>,
    pub owner: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbTransactionStatusMeta {
    pub error: Option<DbTransactionError>,
    pub fee: i64,
    pub pre_balances: Vec<i64>,
    pub post_balances: Vec<i64>,
    pub inner_instructions: Option<Vec<DbInnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<DbTransactionTokenBalance>>,
    pub post_token_balances: Option<Vec<DbTransactionTokenBalance>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbTransactionMessage {
    pub account_keys: Vec<String>,
    pub instructions: Vec<DbCompiledInstruction>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbTransaction {
    pub signature: String,
    pub block_time: u64,
    pub slot: i64,
    pub message: Option<DbTransactionMessage>,
    pub meta: DbTransactionStatusMeta,
    pub signatures: Vec<String>,
    pub write_version: i64,
    pub index: i64,
}

pub struct LogTransactionRequest {
    pub transaction_info: DbTransaction,
}

impl From<&CompiledInstruction> for DbCompiledInstruction {
    fn from(instruction: &CompiledInstruction) -> Self {
        Self {
            program_id_index: instruction.program_id_index as i16,
            accounts: instruction
                .accounts
                .iter()
                .map(|account_idx| *account_idx as i16)
                .collect(),
            data: bs58::encode(instruction.data.clone()).into_string(),
        }
    }
}

impl From<&Message> for DbTransactionMessage {
    fn from(message: &Message) -> Self {
        Self {
            account_keys: message
                .account_keys
                .iter()
                .map(|key| bs58::encode(key.as_ref().to_vec()).into_string())
                .collect(),
            instructions: message
                .instructions
                .iter()
                .map(DbCompiledInstruction::from)
                .collect(),
        }
    }
}

impl From<&v0::Message> for DbTransactionMessage {
    fn from(message: &v0::Message) -> Self {
        Self {
            account_keys: message
                .account_keys
                .iter()
                .map(|key| bs58::encode(key.as_ref().to_vec()).into_string())
                .collect(),
            instructions: message
                .instructions
                .iter()
                .map(DbCompiledInstruction::from)
                .collect(),

        }
    }
}

impl From<&InnerInstructions> for DbInnerInstructions {
    fn from(instructions: &InnerInstructions) -> Self {
        Self {
            index: instructions.index as i16,
            instructions: instructions
                .instructions
                .iter()
                .map(|instruction| DbCompiledInstruction::from(&instruction.instruction))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum DbTransactionErrorCode {
    AccountInUse,
    AccountLoadedTwice,
    AccountNotFound,
    ProgramAccountNotFound,
    InsufficientFundsForFee,
    InvalidAccountForFee,
    AlreadyProcessed,
    BlockhashNotFound,
    InstructionError,
    CallChainTooDeep,
    MissingSignatureForFee,
    InvalidAccountIndex,
    SignatureFailure,
    InvalidProgramForExecution,
    SanitizeFailure,
    ClusterMaintenance,
    AccountBorrowOutstanding,
    WouldExceedMaxAccountCostLimit,
    WouldExceedMaxBlockCostLimit,
    UnsupportedVersion,
    InvalidWritableAccount,
    WouldExceedMaxAccountDataCostLimit,
    TooManyAccountLocks,
    AddressLookupTableNotFound,
    InvalidAddressLookupTableOwner,
    InvalidAddressLookupTableData,
    InvalidAddressLookupTableIndex,
    InvalidRentPayingAccount,
    WouldExceedMaxVoteCostLimit,
    WouldExceedAccountDataBlockLimit,
    WouldExceedAccountDataTotalLimit,
    DuplicateInstruction,
    InsufficientFundsForRent,
    MaxLoadedAccountsDataSizeExceeded,
    InvalidLoadedAccountsDataSizeLimit,
    ResanitizationNeeded,
    UnbalancedTransaction,
    ProgramExecutionTemporarilyRestricted,
    ProgramCacheHitMaxLimit,
}

impl From<&TransactionError> for DbTransactionErrorCode {
    fn from(err: &TransactionError) -> Self {
        match err {
            TransactionError::AccountInUse => Self::AccountInUse,
            TransactionError::AccountLoadedTwice => Self::AccountLoadedTwice,
            TransactionError::AccountNotFound => Self::AccountNotFound,
            TransactionError::ProgramAccountNotFound => Self::ProgramAccountNotFound,
            TransactionError::InsufficientFundsForFee => Self::InsufficientFundsForFee,
            TransactionError::InvalidAccountForFee => Self::InvalidAccountForFee,
            TransactionError::AlreadyProcessed => Self::AlreadyProcessed,
            TransactionError::BlockhashNotFound => Self::BlockhashNotFound,
            TransactionError::InstructionError(_idx, _error) => Self::InstructionError,
            TransactionError::CallChainTooDeep => Self::CallChainTooDeep,
            TransactionError::MissingSignatureForFee => Self::MissingSignatureForFee,
            TransactionError::InvalidAccountIndex => Self::InvalidAccountIndex,
            TransactionError::SignatureFailure => Self::SignatureFailure,
            TransactionError::InvalidProgramForExecution => Self::InvalidProgramForExecution,
            TransactionError::SanitizeFailure => Self::SanitizeFailure,
            TransactionError::ClusterMaintenance => Self::ClusterMaintenance,
            TransactionError::AccountBorrowOutstanding => Self::AccountBorrowOutstanding,
            TransactionError::WouldExceedMaxAccountCostLimit => {
                Self::WouldExceedMaxAccountCostLimit
            }
            TransactionError::WouldExceedMaxBlockCostLimit => Self::WouldExceedMaxBlockCostLimit,
            TransactionError::UnsupportedVersion => Self::UnsupportedVersion,
            TransactionError::InvalidWritableAccount => Self::InvalidWritableAccount,
            TransactionError::WouldExceedAccountDataBlockLimit => {
                Self::WouldExceedAccountDataBlockLimit
            }
            TransactionError::WouldExceedAccountDataTotalLimit => {
                Self::WouldExceedAccountDataTotalLimit
            }
            TransactionError::TooManyAccountLocks => Self::TooManyAccountLocks,
            TransactionError::AddressLookupTableNotFound => Self::AddressLookupTableNotFound,
            TransactionError::InvalidAddressLookupTableOwner => {
                Self::InvalidAddressLookupTableOwner
            }
            TransactionError::InvalidAddressLookupTableData => Self::InvalidAddressLookupTableData,
            TransactionError::InvalidAddressLookupTableIndex => {
                Self::InvalidAddressLookupTableIndex
            }
            TransactionError::InvalidRentPayingAccount => Self::InvalidRentPayingAccount,
            TransactionError::WouldExceedMaxVoteCostLimit => Self::WouldExceedMaxVoteCostLimit,
            TransactionError::DuplicateInstruction(_) => Self::DuplicateInstruction,
            TransactionError::InsufficientFundsForRent { account_index: _ } => {
                Self::InsufficientFundsForRent
            }
            TransactionError::MaxLoadedAccountsDataSizeExceeded => {
                Self::MaxLoadedAccountsDataSizeExceeded
            }
            TransactionError::InvalidLoadedAccountsDataSizeLimit => {
                Self::InvalidLoadedAccountsDataSizeLimit
            }
            TransactionError::ResanitizationNeeded => Self::ResanitizationNeeded,
            TransactionError::UnbalancedTransaction => Self::UnbalancedTransaction,
            TransactionError::ProgramExecutionTemporarilyRestricted { account_index: _ } => {
                Self::ProgramExecutionTemporarilyRestricted
            }
            TransactionError::ProgramCacheHitMaxLimit => Self::ProgramCacheHitMaxLimit,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DbTransactionError {
    error_code: DbTransactionErrorCode,
    error_detail: Option<String>,
}

fn get_transaction_error(result: &Result<(), TransactionError>) -> Option<DbTransactionError> {
    if result.is_ok() {
        return None;
    }

    let error = result.as_ref().err().unwrap();
    Some(DbTransactionError {
        error_code: DbTransactionErrorCode::from(error),
        error_detail: {
            if let TransactionError::InstructionError(idx, instruction_error) = error {
                let mut error_detail = format!(
                    "InstructionError: idx ({}), error: ({})",
                    idx, instruction_error
                );
                if error_detail.len() > MAX_TRANSACTION_STATUS_LEN {
                    error_detail = error_detail
                        .to_string()
                        .split_off(MAX_TRANSACTION_STATUS_LEN);
                }
                Some(error_detail)
            } else {
                None
            }
        },
    })
}

impl From<&TransactionTokenBalance> for DbTransactionTokenBalance {
    fn from(token_balance: &TransactionTokenBalance) -> Self {
        Self {
            account_index: token_balance.account_index as i16,
            mint: token_balance.mint.clone(),
            ui_token_amount: token_balance.ui_token_amount.ui_amount,
            owner: token_balance.owner.clone(),
        }
    }
}

impl From<&TransactionStatusMeta> for DbTransactionStatusMeta {
    fn from(meta: &TransactionStatusMeta) -> Self {
        Self {
            error: get_transaction_error(&meta.status),
            fee: meta.fee as i64,
            pre_balances: meta
                .pre_balances
                .iter()
                .map(|balance| *balance as i64)
                .collect(),
            post_balances: meta
                .post_balances
                .iter()
                .map(|balance| *balance as i64)
                .collect(),
            inner_instructions: meta
                .inner_instructions
                .as_ref()
                .map(|instructions| instructions.iter().map(DbInnerInstructions::from).collect()),
            log_messages: meta.log_messages.clone(),
            pre_token_balances: meta.pre_token_balances.as_ref().map(|balances| {
                balances
                    .iter()
                    .map(DbTransactionTokenBalance::from)
                    .collect()
            }),
            post_token_balances: meta.post_token_balances.as_ref().map(|balances| {
                balances
                    .iter()
                    .map(DbTransactionTokenBalance::from)
                    .collect()
            }),
            // rewards: meta
            //     .rewards
            //     .as_ref()
            //     .map(|rewards| rewards.iter().map(DbReward::from).collect()),
        }
    }
}

fn build_db_transaction(
    slot: u64,
    transaction_info: &ReplicaTransactionInfoV2,
    transaction_write_version: u64,
) -> DbTransaction {
    DbTransaction {
        signature: bs58::encode(transaction_info.signature.as_ref()).into_string(),
        //is_vote: transaction_info.is_vote,
        slot: slot as i64,
        // message_type: match transaction_info.transaction.message() {
        //     SanitizedMessage::Legacy(_) => 0,
        //     SanitizedMessage::V0(_) => 1,
        // },
        message: match transaction_info.transaction.message() {
            SanitizedMessage::Legacy(legacy_message) => {
                Some(DbTransactionMessage::from(legacy_message.message.as_ref()))
            }
            SanitizedMessage::V0(loaded_message) => Some(DbTransactionMessage::from(loaded_message))
        },
        signatures: transaction_info
            .transaction
            .signatures()
            .iter()
            .map(|signature| bs58::encode(signature.as_ref().to_vec()).into_string())
            .collect(),
        meta: DbTransactionStatusMeta::from(transaction_info.transaction_status_meta),
        write_version: transaction_write_version as i64,
        index: transaction_info.index as i64,
        block_time: std::time::UNIX_EPOCH.elapsed().unwrap().as_secs()
    }
}

impl SimplePostgresClient {
    pub(crate) fn log_transaction_impl(
        &mut self,
        transaction_log_info: LogTransactionRequest,
    ) -> Result<(), GeyserPluginError> {
        let client = self.client.get_mut().unwrap();
        let redis = &mut client.redis;
        //let updated_on = Utc::now().naive_utc();

        let transaction_info = transaction_log_info.transaction_info;
        let queue_name = String::from("solana:pools");
        let data = serde_json::to_string(&transaction_info).unwrap();
        let result = redis.lpush::<String, String, String>(queue_name, data);
        if let Err(err) = result {
            let msg = format!(
                "Failed to persist the update of transaction info to the Redis database. Error: {:?}",
                err
            );
            error!("{}", msg);
            return Err(GeyserPluginError::AccountsUpdateError { msg });
        }

        Ok(())
    }
}

impl ParallelPostgresClient {
    fn build_transaction_request(
        slot: u64,
        transaction_info: &ReplicaTransactionInfoV2,
        transaction_write_version: u64,
    ) -> LogTransactionRequest {
        LogTransactionRequest {
            transaction_info: build_db_transaction(
                slot,
                transaction_info,
                transaction_write_version,
            ),
        }
    }

    pub fn log_transaction_info(
        &self,
        transaction_info: &ReplicaTransactionInfoV2,
        slot: u64,
    ) -> Result<(), GeyserPluginError> {
        self.transaction_write_version
            .fetch_add(1, Ordering::Relaxed);
        let wrk_item = DbWorkItem::LogTransaction(Box::new(Self::build_transaction_request(
            slot,
            transaction_info,
            self.transaction_write_version.load(Ordering::Relaxed),
        )));

        if let Err(err) = self.sender.send(wrk_item) {
            return Err(GeyserPluginError::SlotStatusUpdateError {
                msg: format!("Failed to update the transaction, error: {:?}", err),
            });
        }
        Ok(())
    }
}
