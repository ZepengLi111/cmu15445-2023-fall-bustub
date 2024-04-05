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

#include <unistd.h>
#include <sstream>
#include <thread>
#include "execution/executors/update_executor.h"
//#include <string_view>

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto index : index_infos_) {
    if (index->is_primary_key_) {
      primary_key_index_ = index;
      break;
    }
  }
  count_ = 0;
}

void UpdateExecutor::Init() {
  count_ = 0;
  is_finished_ = false;
  child_executor_->Init();
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }
  Tuple old_tuple{};
  RID old_rid;
  auto status = child_executor_->Next(&old_tuple, &old_rid);
  bool modify_primary = false;
  while (status) {
    is_finished_ = true;
    bool self_modify = CheckSelfModify(table_info_->table_->GetTupleMeta(old_rid), exec_ctx_->GetTransaction());
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
    new_tuple.SetRid(old_rid);
    if (primary_key_index_ != nullptr) {
      modify_primary = CheckModifyPrimaryKey(old_tuple, new_tuple, primary_key_index_, table_info_);
      if (modify_primary) {
        break;
      }
    }
    if (self_modify) {
      auto undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid);
      if (undo_link.has_value() && undo_link.value().IsValid()) {
        auto new_undo_log = UpdateOldUndoLog(exec_ctx_->GetTransaction()->GetUndoLog(undo_link->prev_log_idx_),
                                             old_tuple, new_tuple, &child_executor_->GetOutputSchema(),
                                             table_info_->table_->GetTupleMeta(old_rid).is_deleted_, false);
        exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
      }
      table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, new_tuple,
                                              old_rid, nullptr);
    } else {
      auto mark_in_process_success = MarkUndoVersionLink(exec_ctx_, old_rid);
      if (!mark_in_process_success) {
        //        fmt::println(stderr, "update-3-mark-fail RID={}/{} tid {}", old_rid.GetPageId(), old_rid.GetSlotNum(),
        //        ss2.str());
        // 标记in process 失败，有一个线程正在in process
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("update: marking 'in process' fails");
      }
      bool ww_conflict = CheckWWConflict(table_info_->table_->GetTupleMeta(old_rid), exec_ctx_->GetTransaction());
      if (ww_conflict) {
        //        fmt::println(stderr, "update-4-ww-conflict RID={}/{} tid {}", old_rid.GetPageId(),
        //        old_rid.GetSlotNum(), ss2.str());
        // 写写冲突
        UnmarkUndoVersionLink(exec_ctx_, old_rid);
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("update ww conflict");
      }
      //      std::stringstream ss;
      //      ss << std::this_thread::get_id();
      //      fmt::println(stderr, "update-5-mark-success RID={}/{} tid {}", old_rid.GetPageId(), old_rid.GetSlotNum(),
      //      ss.str());
      auto undo_log = GenerateUndoLog(old_tuple, new_tuple, &child_executor_->GetOutputSchema(),
                                      table_info_->table_->GetTupleMeta(old_rid).is_deleted_, false,
                                      table_info_->table_->GetTupleMeta(old_rid).ts_);
      // TO DO check
      //      if (!exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid).value().IsValid()) {
      //        fmt::println(stderr, "update-6-invalid-prev RID={}/{} tid {}", old_rid.GetPageId(),
      //        old_rid.GetSlotNum(), ss.str());
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

  if (modify_primary) {
    std::vector<std::pair<Tuple, RID>> old_tuple_rids;
    std::vector<Tuple> new_tuples;
    while (status) {
      old_tuple_rids.emplace_back(old_tuple, old_rid);
      auto self_modify = CheckSelfModify(table_info_->table_->GetTupleMeta(old_rid), exec_ctx_->GetTransaction());
      // 第一步，删除旧元组
      if (self_modify) {
        // 自我修改
        // TO DO 如果是自己insert并且delete，是否需要从write set中删掉呢
        auto undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid);
        if (undo_link.has_value() && undo_link.value().IsValid()) {
          auto new_undo_log = UpdateOldUndoLog(exec_ctx_->GetTransaction()->GetUndoLog(undo_link->prev_log_idx_),
                                               old_tuple, {}, &child_executor_->GetOutputSchema(), false, true);
          exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
        }
        table_info_->table_->UpdateTupleMeta(TupleMeta{exec_ctx_->GetTransaction()->GetTransactionTempTs(), true},
                                             old_rid);
      } else {
        auto mark_in_process_success = MarkUndoVersionLink(exec_ctx_, old_rid);
        if (!mark_in_process_success) {
          // 标记in process 失败，有一个线程正在in process
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("update: marking 'in process' fails");
        }
        bool ww_conflict = CheckWWConflict(table_info_->table_->GetTupleMeta(old_rid), exec_ctx_->GetTransaction());
        if (ww_conflict) {
          // 写写冲突
          UnmarkUndoVersionLink(exec_ctx_, old_rid);
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("update ww conflict");
        }
        auto undo_log = GenerateUndoLog(old_tuple, {}, &child_executor_->GetOutputSchema(), false, true,
                                        table_info_->table_->GetTupleMeta(old_rid).ts_);
        // TO DO check
        undo_log.prev_version_ = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid).value();
        auto new_undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
        //      exec_ctx_->GetTransactionManager()->UpdateUndoLink(old_rid, new_undo_link);
        auto vul = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
        BUSTUB_ASSERT(vul.has_value(), "update: vul is nullopt");
        vul->in_progress_ = true;
        exec_ctx_->GetTransactionManager()->UpdateVersionLink(old_rid, vul);
        table_info_->table_->UpdateTupleMeta(TupleMeta{exec_ctx_->GetTransaction()->GetTransactionTempTs(), true},
                                             old_rid);
        UnmarkUndoVersionLink(exec_ctx_, old_rid);
        exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, old_rid);
      }
      status = child_executor_->Next(&old_tuple, &old_rid);
    }
    TxnMgrDbg("check after delete", exec_ctx_->GetTransactionManager(), table_info_, table_info_->table_.get());

    // 第二步，插入新元组
    for (auto [temp_old_tuple, temp_old_rid] : old_tuple_rids) {
      std::vector<Value> values{};
      values.reserve(GetOutputSchema().GetColumnCount());
      for (const auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&temp_old_tuple, child_executor_->GetOutputSchema()));
      }
      auto new_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, primary_key_index_->key_schema_,
                                            primary_key_index_->index_->GetKeyAttrs());
      std::vector<RID> result;
      primary_key_index_->index_->ScanKey(new_key, &result, exec_ctx_->GetTransaction());
      bool update_after_delete = false;
      RID update_after_delete_id;
      if (!result.empty()) {
        if (table_info_->table_->GetTupleMeta(result[0]).is_deleted_) {
          update_after_delete = true;
          update_after_delete_id = result[0];
        } else {
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("update: key already in index");
        }
      }
      if (!update_after_delete) {
        auto temp_rid = table_info_->table_->InsertTuple(
            TupleMeta{exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, new_tuple);
        if (temp_rid.has_value()) {
          if (primary_key_index_ != nullptr) {
            auto insert_key_success = primary_key_index_->index_->InsertEntry(
                new_tuple.KeyFromTuple(table_info_->schema_, primary_key_index_->key_schema_,
                                       primary_key_index_->index_->GetKeyAttrs()),
                temp_rid.value(), exec_ctx_->GetTransaction());
            if (!insert_key_success) {
              table_info_->table_->UpdateTupleMeta({exec_ctx_->GetTransaction()->GetTransactionTempTs(), true},
                                                   temp_rid.value());
              exec_ctx_->GetTransaction()->SetTainted();
              throw ExecutionException("insert: inserting key fails");
            }
          }
          exec_ctx_->GetTransactionManager()->UpdateUndoLink(temp_rid.value(), UndoLink{}, nullptr);
          exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, temp_rid.value());
        }
      } else {
        bool self_modify =
            CheckSelfModify(table_info_->table_->GetTupleMeta(update_after_delete_id), exec_ctx_->GetTransaction());
        if (self_modify) {
          //        fmt::println(stderr, "**************** insert after delete and self modify");
          // 自我修改
          auto undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(update_after_delete_id);
          if (undo_link.has_value() && undo_link.value().IsValid()) {
            auto new_undo_log = UpdateOldUndoLog(exec_ctx_->GetTransaction()->GetUndoLog(undo_link->prev_log_idx_), {},
                                                 {}, &child_executor_->GetOutputSchema(), true, false);
            exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
          }
          new_tuple.SetRid(update_after_delete_id);
          table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false},
                                                  new_tuple, update_after_delete_id, nullptr);
        } else {
          bool ww_conflict =
              CheckWWConflict(table_info_->table_->GetTupleMeta(update_after_delete_id), exec_ctx_->GetTransaction());
          if (ww_conflict) {
            // 写写冲突
            UnmarkUndoVersionLink(exec_ctx_, update_after_delete_id);
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("insert: insert after delete ww conflict");
          }
          auto mark_in_process_success = MarkUndoVersionLink(exec_ctx_, update_after_delete_id);
          if (!mark_in_process_success) {
            // 标记in process 失败，有一个线程正在in process
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("delete: marking 'in process' fails");
          }
          auto undo_log = GenerateUndoLog({}, {}, &child_executor_->GetOutputSchema(), true, false,
                                          table_info_->table_->GetTupleMeta(update_after_delete_id).ts_);
          // TO DO check
          undo_log.prev_version_ = exec_ctx_->GetTransactionManager()->GetUndoLink(update_after_delete_id).value();
          auto new_undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
          auto vul = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
          BUSTUB_ASSERT(vul.has_value(), "insert: vul is nullopt");
          vul->in_progress_ = true;
          exec_ctx_->GetTransactionManager()->UpdateVersionLink(update_after_delete_id, vul);
          //        exec_ctx_->GetTransactionManager()->UpdateUndoLink(insert_after_delete_rid, new_undo_link);
          new_tuple.SetRid(update_after_delete_id);
          table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false},
                                                  new_tuple, update_after_delete_id, nullptr);
          UnmarkUndoVersionLink(exec_ctx_, update_after_delete_id);
        }
        // TO DO check
        table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, new_tuple,
                                                update_after_delete_id, nullptr);
        exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, update_after_delete_id);
      }
    }
  }

  if (!is_finished_) {
    // 没有成功改变tuple
    is_finished_ = true;
    return false;
  }
  std::vector<Value> num_of_rows;
  num_of_rows.emplace_back(INTEGER, count_);
  *tuple = Tuple(num_of_rows, &GetOutputSchema());
  return true;
}

}  // namespace bustub
