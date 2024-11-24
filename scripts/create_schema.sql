/**
 * This plugin implementation for PostgreSQL requires the following tables
 */

-- Types for Transactions

Create TYPE "TransactionErrorCode" AS ENUM (
    'AccountInUse',
    'AccountLoadedTwice',
    'AccountNotFound',
    'ProgramAccountNotFound',
    'InsufficientFundsForFee',
    'InvalidAccountForFee',
    'AlreadyProcessed',
    'BlockhashNotFound',
    'InstructionError',
    'CallChainTooDeep',
    'MissingSignatureForFee',
    'InvalidAccountIndex',
    'SignatureFailure',
    'InvalidProgramForExecution',
    'SanitizeFailure',
    'ClusterMaintenance',
    'AccountBorrowOutstanding',
    'WouldExceedMaxAccountCostLimit',
    'WouldExceedMaxBlockCostLimit',
    'UnsupportedVersion',
    'InvalidWritableAccount',
    'WouldExceedMaxAccountDataCostLimit',
    'TooManyAccountLocks',
    'AddressLookupTableNotFound',
    'InvalidAddressLookupTableOwner',
    'InvalidAddressLookupTableData',
    'InvalidAddressLookupTableIndex',
    'InvalidRentPayingAccount',
    'WouldExceedMaxVoteCostLimit',
    'WouldExceedAccountDataBlockLimit',
    'WouldExceedAccountDataTotalLimit',
    'DuplicateInstruction',
    'InsufficientFundsForRent',
    'MaxLoadedAccountsDataSizeExceeded',
    'InvalidLoadedAccountsDataSizeLimit',
    'ResanitizationNeeded',
    'UnbalancedTransaction',
    'ProgramExecutionTemporarilyRestricted',
    'ProgramCacheHitMaxLimit'
);

CREATE TYPE "TransactionError" AS (
    error_code "TransactionErrorCode",
    error_detail VARCHAR(256)
);

CREATE TYPE "CompiledInstruction" AS (
    program_id_index SMALLINT,
    accounts SMALLINT[],
    data BYTEA
);

CREATE TYPE "InnerInstructions" AS (
    index SMALLINT,
    instructions "CompiledInstruction"[]
);

CREATE TYPE "TransactionTokenBalance" AS (
    account_index SMALLINT,
    mint VARCHAR(44),
    ui_token_amount DOUBLE PRECISION,
    owner VARCHAR(44)
);

Create TYPE "RewardType" AS ENUM (
    'Fee',
    'Rent',
    'Staking',
    'Voting'
);

CREATE TYPE "Reward" AS (
    pubkey VARCHAR(44),
    lamports BIGINT,
    post_balance BIGINT,
    reward_type "RewardType",
    commission SMALLINT
);

CREATE TYPE "TransactionStatusMeta" AS (
    error "TransactionError",
    fee BIGINT,
    pre_balances BIGINT[],
    post_balances BIGINT[],
    inner_instructions "InnerInstructions"[],
    log_messages TEXT[],
    pre_token_balances "TransactionTokenBalance"[],
    post_token_balances "TransactionTokenBalance"[],
    rewards "Reward"[]
);

CREATE TYPE "TransactionMessageHeader" AS (
    num_required_signatures SMALLINT,
    num_readonly_signed_accounts SMALLINT,
    num_readonly_unsigned_accounts SMALLINT
);

CREATE TYPE "TransactionMessage" AS (
    header "TransactionMessageHeader",
    account_keys BYTEA[],
    recent_blockhash BYTEA,
    instructions "CompiledInstruction"[]
);

CREATE TYPE "TransactionMessageAddressTableLookup" AS (
    account_key BYTEA,
    writable_indexes SMALLINT[],
    readonly_indexes SMALLINT[]
);

CREATE TYPE "TransactionMessageV0" AS (
    header "TransactionMessageHeader",
    account_keys BYTEA[],
    recent_blockhash BYTEA,
    instructions "CompiledInstruction"[],
    address_table_lookups "TransactionMessageAddressTableLookup"[]
);

CREATE TYPE "LoadedAddresses" AS (
    writable BYTEA[],
    readonly BYTEA[]
);

CREATE TYPE "LoadedMessageV0" AS (
    message "TransactionMessageV0",
    loaded_addresses "LoadedAddresses"
);

-- The table storing transactions
CREATE TABLE transaction (
    slot BIGINT NOT NULL,
    signature BYTEA NOT NULL,
    is_vote BOOL NOT NULL,
    message_type SMALLINT, -- 0: legacy, 1: v0 message
    legacy_message "TransactionMessage",
    v0_loaded_message "LoadedMessageV0",
    signatures BYTEA[],
    message_hash BYTEA,
    meta "TransactionStatusMeta",
    write_version BIGINT,
    updated_on TIMESTAMP NOT NULL,
    index BIGINT NOT NULL,
    CONSTRAINT transaction_pk PRIMARY KEY (slot, signature)
);
