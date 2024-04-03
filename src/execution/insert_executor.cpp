//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto index : index_infos_) {
    if (index->is_primary_key_) {
      primary_key_index_ = index;
      break;
    }
  }
}

void InsertExecutor::Init() {
  is_finished_= false;
  count_ = 0;
  child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }
  Tuple child_tuple{};
  RID child_rid;
  RID insert_after_delete_rid;
  auto status = child_executor_->Next(&child_tuple, &child_rid);
  while (status) {
    is_finished_ = true;
    bool insert_after_delete = false;
    std::vector<RID> result;
    if (primary_key_index_ != nullptr) {
      primary_key_index_->index_->ScanKey(
          child_tuple.KeyFromTuple(table_info_->schema_, primary_key_index_->key_schema_,
                                   primary_key_index_->index_->GetKeyAttrs()),
          &result, exec_ctx_->GetTransaction());
      if (!result.empty()) {
        if (table_info_->table_->GetTupleMeta(result[0]).is_deleted_) {
          insert_after_delete_rid = result[0];
          insert_after_delete = true;
        }
        else {
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("insert: tuple already in index");
        }
      }
    }

    TupleMeta tuple_meta;
    tuple_meta.ts_ = exec_ctx_->GetTransaction()->GetTransactionTempTs();
    tuple_meta.is_deleted_ = false;

    if (!insert_after_delete) {
      auto temp_rid = table_info_->table_->InsertTuple(tuple_meta, child_tuple);
      if (temp_rid.has_value()) {
        if (primary_key_index_ != nullptr) {
          auto insert_key_success = primary_key_index_->index_->InsertEntry(
              child_tuple.KeyFromTuple(table_info_->schema_, primary_key_index_->key_schema_,
                                       primary_key_index_->index_->GetKeyAttrs()),
              temp_rid.value(), exec_ctx_->GetTransaction());
          if (!insert_key_success) {
            table_info_->table_->UpdateTupleMeta({tuple_meta.ts_, true}, temp_rid.value());
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("insert: inserting key fails");
          }
        }
        exec_ctx_->GetTransactionManager()->UpdateUndoLink(temp_rid.value(), UndoLink{}, nullptr);
        exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, temp_rid.value());
      }
    }
    else {
      bool self_modify = CheckSelfModify(table_info_->table_->GetTupleMeta(insert_after_delete_rid), exec_ctx_->GetTransaction());
      if (self_modify) {
//        fmt::println(stderr, "**************** insert after delete and self modify");
        // 自我修改
        auto undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(insert_after_delete_rid);
        if (undo_link.has_value() && undo_link.value().IsValid()) {
          auto new_undo_log = UpdateOldUndoLog(exec_ctx_->GetTransaction()->GetUndoLog(undo_link->prev_log_idx_),
                                               {}, {}, &child_executor_->GetOutputSchema(), true, false);
          exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
        }
        table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, child_tuple,
                                                insert_after_delete_rid, nullptr);
      }
      else {
        bool ww_conflict = CheckWWConflict(table_info_->table_->GetTupleMeta(insert_after_delete_rid), exec_ctx_->GetTransaction());

        if (ww_conflict) {
          // 写写冲突
          UnmarkUndoVersionLink(exec_ctx_, insert_after_delete_rid);
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("insert: insert after delete ww conflict");
        }

        auto mark_in_process_success = MarkUndoVersionLink(exec_ctx_, insert_after_delete_rid);
        if (!mark_in_process_success) {
          // 标记in process 失败，有一个线程正在in process
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("delete: marking 'in process' fails");
        }
        auto undo_log = GenerateUndoLog({}, {}, &child_executor_->GetOutputSchema(), true, false,
                                        table_info_->table_->GetTupleMeta(insert_after_delete_rid).ts_);
        // TO DO check
        undo_log.prev_version_ = exec_ctx_->GetTransactionManager()->GetUndoLink(insert_after_delete_rid).value();
        auto new_undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
        auto vul = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
        BUSTUB_ASSERT(vul.has_value(), "insert: vul is nullopt");
        vul->in_progress_ = true;
        exec_ctx_->GetTransactionManager()->UpdateVersionLink(insert_after_delete_rid, vul);
//        exec_ctx_->GetTransactionManager()->UpdateUndoLink(insert_after_delete_rid, new_undo_link);
        table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, child_tuple,
                                                insert_after_delete_rid, nullptr);
        UnmarkUndoVersionLink(exec_ctx_, insert_after_delete_rid);
      }
      // TO DO check
      table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, child_tuple,
                                              insert_after_delete_rid, nullptr);
      exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, insert_after_delete_rid);
    }
    count_++;
    status = child_executor_->Next(&child_tuple, &child_rid);
  }
  if (!is_finished_) {
    // 没有成功改变的tuple
    is_finished_ = true;
    return false;
  }
  std::vector<Value> num_of_rows;
  num_of_rows.emplace_back(INTEGER, count_);
  *tuple = Tuple(num_of_rows, &GetOutputSchema());
  return true;
}

}  // namespace bustub
