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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  count_ = 0;
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
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
    TupleMeta tuple_meta;
    tuple_meta.ts_ = 0;
    tuple_meta.is_deleted_ = false;
    auto temp_rid = table_info_->table_->InsertTuple(tuple_meta, child_tuple);
    if (temp_rid.has_value()) {
      for (auto *index_info : index_infos_) {
        index_info->index_->InsertEntry(
            child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
            temp_rid.value(), exec_ctx_->GetTransaction());
      }
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
