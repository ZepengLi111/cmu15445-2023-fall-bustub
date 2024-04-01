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
  count_ = 0;
  child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }
  Tuple child_tuple{};
  RID child_rid;
  auto status = child_executor_->Next(&child_tuple, &child_rid);
  is_finished_ = true;
  while (status) {
    std::vector<RID> result;

    if (primary_key_index_ != nullptr) {
      primary_key_index_->index_->ScanKey(
          child_tuple.KeyFromTuple(table_info_->schema_, primary_key_index_->key_schema_,
                                   primary_key_index_->index_->GetKeyAttrs()),
          &result, exec_ctx_->GetTransaction());
      if (!result.empty()) {
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("insert: tuple already in index");
      }
    }

    TupleMeta tuple_meta;
    tuple_meta.ts_ = exec_ctx_->GetTransaction()->GetTransactionTempTs();
    tuple_meta.is_deleted_ = false;

    auto temp_rid = table_info_->table_->InsertTuple(tuple_meta, child_tuple);
    if (temp_rid.has_value()) {
      if (primary_key_index_ != nullptr) {
        auto insert_key_success = primary_key_index_->index_->InsertEntry(
            child_tuple.KeyFromTuple(table_info_->schema_, primary_key_index_->key_schema_,
                                     primary_key_index_->index_->GetKeyAttrs()),
            child_rid, exec_ctx_->GetTransaction());
        if (!insert_key_success) {
          table_info_->table_->UpdateTupleMeta({tuple_meta.ts_, true}, temp_rid.value());
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("insert: inserting key fails");
        }
      }

      exec_ctx_->GetTransactionManager()->UpdateUndoLink(temp_rid.value(), UndoLink{}, nullptr);
      exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, temp_rid.value());
      status = child_executor_->Next(&child_tuple, &child_rid);
      count_++;
    }
  }
  std::vector<Value> num_of_rows;
  num_of_rows.emplace_back(INTEGER, count_);
  *tuple = Tuple(num_of_rows, &GetOutputSchema());
  return true;
}

}  // namespace bustub
