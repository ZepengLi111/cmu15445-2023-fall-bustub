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
#include <unistd.h>
#include <thread>
#include <sstream>
//#include <string_view>

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
  is_finished_= false;
  child_executor_->Init();
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }
  Tuple old_tuple{};
  RID old_rid;
  auto status = child_executor_->Next(&old_tuple, &old_rid);
//  std::stringstream ss2;
//  ss2 << std::this_thread::get_id();
//  fmt::println(stderr, "update-1 RID={}/{} tid {}", old_rid.GetPageId(), old_rid.GetSlotNum(), ss2.str());
  while (status) {
    is_finished_ = true;
//    std::stringstream ss1;
//    ss1 << std::this_thread::get_id();
//    fmt::println(stderr, "update-2-in-loop RID={}/{} tid {}", old_rid.GetPageId(), old_rid.GetSlotNum(), ss1.str());
    bool self_modify = CheckSelfModify(table_info_->table_->GetTupleMeta(old_rid), exec_ctx_->GetTransaction());
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
    new_tuple.SetRid(old_rid);

    if (self_modify) {
//      fmt::println("modify own CHECK");
      // 自我修改
      auto undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid);
      if (undo_link.has_value() && undo_link.value().IsValid()) {
        auto new_undo_log = UpdateOldUndoLog(exec_ctx_->GetTransaction()->GetUndoLog(undo_link->prev_log_idx_),
                                             old_tuple, new_tuple, &child_executor_->GetOutputSchema(),
                                             table_info_->table_->GetTupleMeta(old_rid).is_deleted_, false);
        exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
      }
      table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, new_tuple,
                                              old_rid, nullptr);
    }
    else {
      auto mark_in_process_success = MarkUndoVersionLink(exec_ctx_, old_rid);
      if (!mark_in_process_success) {
//        fmt::println(stderr, "update-3-mark-fail RID={}/{} tid {}", old_rid.GetPageId(), old_rid.GetSlotNum(), ss2.str());
        // 标记in process 失败，有一个线程正在in process
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("update: marking 'in process' fails");
      }
      bool ww_conflict = CheckWWConflict(table_info_->table_->GetTupleMeta(old_rid), exec_ctx_->GetTransaction());
      if (ww_conflict) {
//        fmt::println(stderr, "update-4-ww-conflict RID={}/{} tid {}", old_rid.GetPageId(), old_rid.GetSlotNum(), ss2.str());
        // 写写冲突
        UnmarkUndoVersionLink(exec_ctx_, old_rid);
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("update ww conflict");
      }
//      std::stringstream ss;
//      ss << std::this_thread::get_id();
//      fmt::println(stderr, "update-5-mark-success RID={}/{} tid {}", old_rid.GetPageId(), old_rid.GetSlotNum(), ss.str());
      auto undo_log = GenerateUndoLog(old_tuple, new_tuple, &child_executor_->GetOutputSchema(),
                                      table_info_->table_->GetTupleMeta(old_rid).is_deleted_, false,
                                      table_info_->table_->GetTupleMeta(old_rid).ts_);
      // TO DO check
//      if (!exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid).value().IsValid()) {
//        fmt::println(stderr, "update-6-invalid-prev RID={}/{} tid {}", old_rid.GetPageId(), old_rid.GetSlotNum(), ss.str());
//      }
      undo_log.prev_version_ = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid).value();
      auto new_undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
      auto vul = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
      BUSTUB_ASSERT(vul.has_value(), "delete: vul is nullopt");
      vul->in_progress_ = true;
      exec_ctx_->GetTransactionManager()->UpdateVersionLink(old_rid, vul);
      table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, new_tuple,
                                              old_rid, nullptr);
      UnmarkUndoVersionLink(exec_ctx_, old_rid);
    }
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, old_rid);
    status = child_executor_->Next(&old_tuple, &old_rid);
    count_++;
  }
  if (!is_finished_) {
//    fmt::println(stderr, "CHECK special tid {}", ss2.str());
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
