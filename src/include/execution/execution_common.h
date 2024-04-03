#pragma once

#include <string>
#include <vector>

#include "concurrency/transaction_manager.h"
#include "catalog/catalog.h"
#include "execution/executor_context.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

class CheckInProcessObj {
 public:
  explicit CheckInProcessObj(VersionUndoLink vul) {
    vul_ = vul;
  }
  auto operator()(std::optional<VersionUndoLink> version_undo_link) const -> bool {
    if (!version_undo_link.has_value()) {
      fmt::println(stderr, "CheckInProcess: has not value");
      return false;
    }
    if (vul_ != version_undo_link.value()) {
      return false;
    }
    if (version_undo_link.value().in_progress_) {
      return false;
    }
    return true;
  }

 private:
  VersionUndoLink vul_;
};

/**
 * @param schema tuple schema, child_executor.GetOutputSchema()
 * **/
auto GenerateUndoLog(const Tuple &old_tuple, const Tuple &new_tuple, const Schema *schema, bool old_is_deleted,
                     bool new_is_deleted, timestamp_t ts) -> UndoLog;

auto UpdateOldUndoLog(const UndoLog &old_log, const Tuple &old_tuple, const Tuple &new_tuple, const Schema *schema,
                      bool old_is_deleted, bool new_is_deleted) -> UndoLog;

auto GetSchemaFromModifiedFields(const std::vector<bool> &modified_fields, const Schema *schema) -> Schema;

auto ConstructDelUndoLog() -> UndoLog;

auto GenerateDeleteUndoLog(Transaction *txn) -> UndoLog;

auto UpdateOldUndoLogToDelete(const UndoLog &undoLog) -> UndoLog;

auto CheckInProcess(std::optional<VersionUndoLink> version_undo_link) -> bool;

auto MarkUndoVersionLink(ExecutorContext * exec_ctx, RID rid) -> bool;

void UnmarkUndoVersionLink(ExecutorContext * exec_ctx, RID rid);

/**
 * @return true if conflict, otherwise false
 * **/
auto CheckWWConflict(const TupleMeta &meta, Transaction *txn) -> bool;

auto CheckSelfModify(const TupleMeta &meta, Transaction *txn) -> bool;

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap, int thread=-1);

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
