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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum DbRewardType {
    Fee,
    Rent,
    Staking,
    Voting,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbReward {
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: i64,
    pub reward_type: Option<DbRewardType>,
    pub commission: Option<i16>,
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
    pub rewards: Option<Vec<DbReward>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbTransactionMessageHeader {
    pub num_required_signatures: i16,
    pub num_readonly_signed_accounts: i16,
    pub num_readonly_unsigned_accounts: i16,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbTransactionMessage {
    pub header: DbTransactionMessageHeader,
    pub account_keys: Vec<String>,
    pub instructions: Vec<DbCompiledInstruction>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbTransactionMessageAddressTableLookup {
    pub account_key: String,
    pub writable_indexes: Vec<i16>,
    pub readonly_indexes: Vec<i16>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbTransactionMessageV0 {
    pub header: DbTransactionMessageHeader,
    pub account_keys: Vec<String>,
    pub instructions: Vec<DbCompiledInstruction>,
    pub address_table_lookups: Vec<DbTransactionMessageAddressTableLookup>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbLoadedMessageV0 {
    pub message: DbTransactionMessageV0,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbTransaction {
    pub signature: String,
    pub is_vote: bool,
    pub slot: i64,
    pub message_type: i16,
    pub legacy_message: Option<DbTransactionMessage>,
    pub v0_loaded_message: Option<DbLoadedMessageV0>,
    pub meta: DbTransactionStatusMeta,
    pub signatures: Vec<String>,
    /// This can be used to tell the order of transaction within a block
    /// Given a slot, the transaction with a smaller write_version appears
    /// before transactions with higher write_versions in a shred.
    pub write_version: i64,
    pub index: i64,
}

pub struct LogTransactionRequest {
    pub transaction_info: DbTransaction,
}

impl From<&MessageAddressTableLookup> for DbTransactionMessageAddressTableLookup {
    fn from(address_table_lookup: &MessageAddressTableLookup) -> Self {
        Self {
            account_key: bs58::encode(address_table_lookup.account_key.as_ref().to_vec()).into_string(),
            writable_indexes: address_table_lookup
                .writable_indexes
                .iter()
                .map(|idx| *idx as i16)
                .collect(),
            readonly_indexes: address_table_lookup
                .readonly_indexes
                .iter()
                .map(|idx| *idx as i16)
                .collect(),
        }
    }
}

impl From<&MessageHeader> for DbTransactionMessageHeader {
    fn from(header: &MessageHeader) -> Self {
        Self {
            num_required_signatures: header.num_required_signatures as i16,
            num_readonly_signed_accounts: header.num_readonly_signed_accounts as i16,
            num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as i16,
        }
    }
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
            header: DbTransactionMessageHeader::from(&message.header),
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

impl From<&v0::Message> for DbTransactionMessageV0 {
    fn from(message: &v0::Message) -> Self {
        Self {
            header: DbTransactionMessageHeader::from(&message.header),
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
            address_table_lookups: message
                .address_table_lookups
                .iter()
                .map(DbTransactionMessageAddressTableLookup::from)
                .collect(),
        }
    }
}

impl<'a> From<&v0::LoadedMessage<'a>> for DbLoadedMessageV0 {
    fn from(message: &v0::LoadedMessage) -> Self {
        Self {
            message: DbTransactionMessageV0::from(&message.message as &v0::Message),
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

impl From<&RewardType> for DbRewardType {
    fn from(reward_type: &RewardType) -> Self {
        match reward_type {
            RewardType::Fee => Self::Fee,
            RewardType::Rent => Self::Rent,
            RewardType::Staking => Self::Staking,
            RewardType::Voting => Self::Voting,
        }
    }
}

fn get_reward_type(reward: &Option<RewardType>) -> Option<DbRewardType> {
    reward.as_ref().map(DbRewardType::from)
}

impl From<&Reward> for DbReward {
    fn from(reward: &Reward) -> Self {
        Self {
            pubkey: reward.pubkey.clone(),
            lamports: reward.lamports,
            post_balance: reward.post_balance as i64,
            reward_type: get_reward_type(&reward.reward_type),
            commission: reward
                .commission
                .as_ref()
                .map(|commission| *commission as i16),
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
            rewards: meta
                .rewards
                .as_ref()
                .map(|rewards| rewards.iter().map(DbReward::from).collect()),
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
        is_vote: transaction_info.is_vote,
        slot: slot as i64,
        message_type: match transaction_info.transaction.message() {
            SanitizedMessage::Legacy(_) => 0,
            SanitizedMessage::V0(_) => 1,
        },
        legacy_message: match transaction_info.transaction.message() {
            SanitizedMessage::Legacy(legacy_message) => {
                Some(DbTransactionMessage::from(legacy_message.message.as_ref()))
            }
            _ => None,
        },
        v0_loaded_message: match transaction_info.transaction.message() {
            SanitizedMessage::V0(loaded_message) => Some(DbLoadedMessageV0::from(loaded_message)),
            _ => None,
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
