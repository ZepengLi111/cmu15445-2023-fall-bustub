#include "execution/execution_common.h"
#include <string>
#include "catalog/catalog.h"
#include "common/config.h"
#include "execution/executors/abstract_executor.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto CheckInProcess(std::optional<VersionUndoLink> version_undo_link) -> bool {
  if (!version_undo_link.has_value()) {
    fmt::println(stderr, "CheckInProcess: has not value");
    return false;
  }
  if (version_undo_link.value().in_progress_) {
    return false;
  }
  return true;
}


/**
 * 如果标记成功返回true，如果标记失败返回false，如果没有undolink也返回true
 * **/
auto MarkUndoVersionLink(ExecutorContext * exec_ctx, RID rid) -> bool {
  auto vul = VersionUndoLink::FromOptionalUndoLink(exec_ctx->GetTransactionManager()->GetUndoLink(rid));
  if (vul.has_value()) {
    CheckInProcessObj check_obj(vul.value());
    vul->in_progress_ = true;
    return exec_ctx->GetTransactionManager()->UpdateVersionLink(
        rid, vul, check_obj);
  }
  fmt::println(stderr, "helplessly mark undo CHECK");
  return true;
}

void UnmarkUndoVersionLink(ExecutorContext * exec_ctx, RID rid) {
  auto vul = VersionUndoLink::FromOptionalUndoLink(exec_ctx->GetTransactionManager()->GetUndoLink(rid));
  if (vul.has_value()) {
    exec_ctx->GetTransactionManager()->UpdateVersionLink(rid, vul, nullptr);
  }
}

auto CheckWWConflict(const TupleMeta &meta, Transaction *txn) -> bool {
  return meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs();
}

auto GenerateUndoLog(const Tuple &old_tuple, const Tuple &new_tuple, const Schema *schema, bool old_is_deleted,
                     bool new_is_deleted, timestamp_t ts) -> UndoLog {
  if (old_is_deleted) {
    return {true, {}, {}, ts};
  }
  if (new_is_deleted) {
    std::vector<bool> modified_fields;
    for (unsigned column_idx = 0; column_idx < schema->GetColumnCount(); column_idx++) {
      modified_fields.push_back(true);
    }
    return {false, modified_fields, old_tuple, ts};
  }

  std::vector<bool> modified_fields;
  std::vector<Value> modified_values;
  std::vector<uint32_t> schema_copy_attrs;
  for (unsigned column_idx = 0; column_idx < schema->GetColumnCount(); column_idx++) {
    if (!old_tuple.GetValue(schema, column_idx).CompareExactlyEquals(new_tuple.GetValue(schema, column_idx))) {
      modified_fields.push_back(true);
      modified_values.push_back(old_tuple.GetValue(schema, column_idx));
      schema_copy_attrs.push_back(column_idx);
    } else {
      modified_fields.push_back(false);
    }
  }
  Schema modified_tuple_schema = Schema::CopySchema(schema, schema_copy_attrs);
  return {false, modified_fields, Tuple{modified_values, &modified_tuple_schema}, ts};
}

auto UpdateOldUndoLog(const UndoLog &old_log, const Tuple &old_tuple, const Tuple &new_tuple, const Schema *schema,
                      bool old_is_deleted, bool new_is_deleted) -> UndoLog {
  if (old_is_deleted || old_log.is_deleted_) {
    // TO DO 这里需要把旧日志的modified_fields需要复制过来吗？
    return old_log;
  }
  if (new_is_deleted) {
    std::vector<bool> modified_fields;
    std::vector<Value> modified_values;
    int log_tuple_column_id = 0;
    for (unsigned column_idx = 0; column_idx < schema->GetColumnCount(); column_idx++) {
      modified_fields.push_back(true);
      if (old_log.modified_fields_[column_idx]) {
        Schema log_tuple_schema = GetSchemaFromModifiedFields(old_log.modified_fields_, schema);
        modified_values.push_back(old_log.tuple_.GetValue(&log_tuple_schema, log_tuple_column_id++));
      } else {
        modified_values.push_back(old_tuple.GetValue(schema, column_idx));
      }
    }
    return {false, modified_fields, Tuple{modified_values, schema}, old_log.ts_, old_log.prev_version_};
  }
  std::vector<bool> modified_fields;
  std::vector<Value> modified_values;
  std::vector<uint32_t> schema_copy_attrs;
  int log_tuple_column_id = 0;
  for (unsigned column_idx = 0; column_idx < schema->GetColumnCount(); column_idx++) {
    if (old_log.modified_fields_[column_idx]) {
      modified_fields.push_back(true);
      Schema log_tuple_schema = GetSchemaFromModifiedFields(old_log.modified_fields_, schema);
      modified_values.push_back(old_log.tuple_.GetValue(&log_tuple_schema, log_tuple_column_id++));
      schema_copy_attrs.push_back(column_idx);
    } else if (!old_tuple.GetValue(schema, column_idx).CompareExactlyEquals(new_tuple.GetValue(schema, column_idx))) {
      modified_fields.push_back(true);
      modified_values.push_back(old_tuple.GetValue(schema, column_idx));
      schema_copy_attrs.push_back(column_idx);
    } else {
      modified_fields.push_back(false);
    }
  }
  Schema modified_tuple_schema = Schema::CopySchema(schema, schema_copy_attrs);
  return {false, modified_fields, Tuple{modified_values, &modified_tuple_schema}, old_log.ts_, old_log.prev_version_};
}

auto GenerateDeleteUndoLog(Transaction *txn) -> UndoLog { return {true, {}, {}, txn->GetTransactionTempTs()}; }

auto UpdateOldUndoLogToDelete(const UndoLog &undoLog) -> UndoLog {
  return {true, undoLog.modified_fields_, undoLog.tuple_, undoLog.ts_};
}

auto CheckSelfModify(const TupleMeta &meta, Transaction *txn) -> bool {
  return meta.ts_ == txn->GetTransactionTempTs();
}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  Tuple reconstructed_tuple = Tuple(base_tuple);

  std::vector<Value> tuple_values;
  for (const auto &column : schema->GetColumns()) {
    tuple_values.emplace_back(base_tuple.GetValue(schema, schema->GetColIdx(column.GetName())));
  }

  bool is_deleted = base_meta.is_deleted_;
  for (const auto &logo : undo_logs) {
    if (logo.is_deleted_) {
      is_deleted = true;
      continue;
    }
    is_deleted = false;

    std::vector<uint32_t> attrs;
    attrs.reserve(logo.modified_fields_.size());
    for (unsigned i = 0; i < logo.modified_fields_.size(); i++) {
      if (logo.modified_fields_[i]) {
        attrs.push_back(i);
      }
    }
    Schema undo_tuple_schema = Schema::CopySchema(schema, attrs);
    int column_id = 0;

    for (unsigned i = 0; i < logo.modified_fields_.size(); i++) {
      if (!logo.modified_fields_[i]) {
        continue;
      }
      tuple_values[i] = logo.tuple_.GetValue(&undo_tuple_schema, column_id);
      column_id++;
    }
  }

  if (is_deleted) {
    return std::nullopt;
  }
  return std::optional<Tuple>{Tuple{tuple_values, schema}};
}

auto GetSchemaFromModifiedFields(const std::vector<bool> &modified_fields, const Schema *schema) -> Schema {
  std::vector<uint32_t> attrs;
  for (unsigned i = 0; i < modified_fields.size(); i++) {
    if (modified_fields[i]) {
      attrs.push_back(i);
    }
  }
  return Schema::CopySchema(schema, attrs);
}

auto ConstructDelUndoLog() -> UndoLog { return {true, {}, {}, {}}; }

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap, int thread) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  //  fmt::println(
  //      stderr,
  //      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //      tasks.");

  fmt::println("");

  auto it = table_heap->MakeIterator();
  while (!it.IsEnd()) {
    auto ts = it.GetTuple().first.ts_;
    fmt::println(stderr, "RID={}/{} ts={} tuple={} is_deleted={} thread={}", it.GetRID().GetPageId(), it.GetRID().GetSlotNum(),
                 ts > TXN_START_ID ? "txn" + std::to_string(ts - TXN_START_ID) : std::to_string(ts),
                 it.GetTuple().second.ToString(&table_info->schema_), it.GetTuple().first.is_deleted_, thread);

    auto undo_link = txn_mgr->GetUndoLink(it.GetRID());
    while (undo_link->IsValid()) {
      if (undo_link.has_value()) {
        auto undo_logo = txn_mgr->GetUndoLogOptional(undo_link.value());
        if (undo_logo.has_value()) {
          Schema schema = GetSchemaFromModifiedFields(undo_logo->modified_fields_, &table_info->schema_);
          fmt::println(stderr, "-------RID={}/{} txn{} {} ts={} is_deleted={} thread={}", it.GetRID().GetPageId(),
                       it.GetRID().GetSlotNum(), undo_link.value().prev_txn_ - TXN_START_ID,
                       undo_logo->tuple_.ToString(&schema), undo_logo->ts_, undo_logo->is_deleted_, thread);
          undo_link = undo_logo->prev_version_;
        } else {
          break;
        }
      } else {
        fmt::println(stderr, "undo link has no value");
        break;
      }
    }
    ++it;
  }

  fmt::println("");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
