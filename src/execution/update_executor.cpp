//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  count_ = 0;
}

void UpdateExecutor::Init() {
  count_ = 0;
  child_executor_->Init();
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }
  Tuple old_tuple{};
  RID old_rid;
  auto status = child_executor_->Next(&old_tuple, &old_rid);
  is_finished_ = true;
  while (status) {
    bool self_modify = CheckSelfModify(table_info_->table_->GetTupleMeta(old_rid), exec_ctx_->GetTransaction());
    bool ww_conflict = CheckWWConflict(table_info_->table_->GetTupleMeta(old_rid), exec_ctx_->GetTransaction());

    if (ww_conflict) {
      // 写写冲突
      exec_ctx_->GetTransaction()->SetTainted();
      //      exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
      throw ExecutionException("update ww conflict");
    }

    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
    new_tuple.SetRid(old_rid);

    if (self_modify) {
      // 自我修改
      auto undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid);
      if (undo_link.has_value() && undo_link.value().IsValid()) {
        auto new_undo_log = UpdateOldUndoLog(exec_ctx_->GetTransaction()->GetUndoLog(undo_link->prev_log_idx_),
                                             old_tuple, new_tuple, &child_executor_->GetOutputSchema(),
                                             table_info_->table_->GetTupleMeta(old_rid).is_deleted_, false);
        exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
      }
    } else {
      auto undo_log = GenerateUndoLog(old_tuple, new_tuple, &child_executor_->GetOutputSchema(),
                                      table_info_->table_->GetTupleMeta(old_rid).is_deleted_, false,
                                      table_info_->table_->GetTupleMeta(old_rid).ts_);
      // TO DO check
      undo_log.prev_version_ = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid).value();
      auto new_undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
      exec_ctx_->GetTransactionManager()->UpdateUndoLink(old_rid, new_undo_link);
    }

    // TO DO check
    table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, new_tuple,
                                            old_rid, nullptr);
    //    table_info_->table_->UpdateTupleMeta()
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, old_rid);
    status = child_executor_->Next(&old_tuple, &old_rid);
    count_++;
  }
  std::vector<Value> num_of_rows;
  num_of_rows.emplace_back(INTEGER, count_);
  *tuple = Tuple(num_of_rows, &GetOutputSchema());
  return true;
}

}  // namespace bustub
