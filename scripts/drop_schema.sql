/**
 * Script for cleaning up the schema for PostgreSQL used for the AccountsDb plugin.
 */

DROP TABLE transaction;

DROP TYPE "TransactionError" CASCADE;
DROP TYPE "TransactionErrorCode" CASCADE;
DROP TYPE "LoadedMessageV0" CASCADE;
DROP TYPE "LoadedAddresses" CASCADE;
DROP TYPE "TransactionMessageV0" CASCADE;
DROP TYPE "TransactionMessage" CASCADE;
DROP TYPE "TransactionMessageHeader" CASCADE;
DROP TYPE "TransactionMessageAddressTableLookup" CASCADE;
DROP TYPE "TransactionStatusMeta" CASCADE;
DROP TYPE "RewardType" CASCADE;
DROP TYPE "Reward" CASCADE;
DROP TYPE "TransactionTokenBalance" CASCADE;
DROP TYPE "InnerInstructions" CASCADE;
DROP TYPE "CompiledInstruction" CASCADE;
