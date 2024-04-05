//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  count_ = 0;
}

void DeleteExecutor::Init() {
  is_finished_ = false;
  count_ = 0;
  child_executor_->Init();
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }
  Tuple old_tuple{};
  RID old_rid;
  auto status = child_executor_->Next(&old_tuple, &old_rid);
  while (status) {
    is_finished_ = true;
    bool self_modify = CheckSelfModify(table_info_->table_->GetTupleMeta(old_rid), exec_ctx_->GetTransaction());

    auto new_meta = TupleMeta{exec_ctx_->GetTransaction()->GetTransactionTempTs(), true};
    //    if (table_info_->table_->GetTupleMeta(old_rid).is_deleted_) {
    //      // 已经删除过了，不能再被删除一遍
    //      // TO DO 可能需要throw Exception
    //      status = child_executor_->Next(&old_tuple, &old_rid);
    //      continue;
    //    }
    if (self_modify) {
      // 自我修改
      // TO DO 如果是自己insert并且delete，是否需要从write set中删掉呢
      auto undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid);
      if (undo_link.has_value() && undo_link.value().IsValid()) {
        auto new_undo_log = UpdateOldUndoLog(exec_ctx_->GetTransaction()->GetUndoLog(undo_link->prev_log_idx_),
                                             old_tuple, {}, &child_executor_->GetOutputSchema(), false, true);
        exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
      }
      table_info_->table_->UpdateTupleMeta(new_meta, old_rid);
    } else {
      auto mark_in_process_success = MarkUndoVersionLink(exec_ctx_, old_rid);
      if (!mark_in_process_success) {
        // 标记in process 失败，有一个线程正在in process
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("delete: marking 'in process' fails");
      }

      bool ww_conflict = CheckWWConflict(table_info_->table_->GetTupleMeta(old_rid), exec_ctx_->GetTransaction());
      if (ww_conflict) {
        // 写写冲突
        UnmarkUndoVersionLink(exec_ctx_, old_rid);
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("delete ww conflict");
      }

      auto undo_log = GenerateUndoLog(old_tuple, {}, &child_executor_->GetOutputSchema(), false, true,
                                      table_info_->table_->GetTupleMeta(old_rid).ts_);
      // TO DO check
      undo_log.prev_version_ = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid).value();
      auto new_undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
      //      exec_ctx_->GetTransactionManager()->UpdateUndoLink(old_rid, new_undo_link);
      auto vul = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
      BUSTUB_ASSERT(vul.has_value(), "delete: vul is nullopt");
      vul->in_progress_ = true;
      exec_ctx_->GetTransactionManager()->UpdateVersionLink(old_rid, vul);
      table_info_->table_->UpdateTupleMeta(new_meta, old_rid);
      UnmarkUndoVersionLink(exec_ctx_, old_rid);
    }

    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, old_rid);
    status = child_executor_->Next(&old_tuple, &old_rid);
    count_++;
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
