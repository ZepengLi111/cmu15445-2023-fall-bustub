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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  count_ = 0;
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }
  Tuple temp_tuple{};
  RID temp_rid;
  auto status = child_executor_->Next(&temp_tuple, &temp_rid);
  is_finished_ = true;
  while (status) {
    auto meta = table_info_->table_->GetTupleMeta(temp_rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, temp_rid);

    for (auto *index_info : index_infos_) {
      index_info->index_->DeleteEntry(
          temp_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          temp_rid, exec_ctx_->GetTransaction());
    }
    status = child_executor_->Next(&temp_tuple, &temp_rid);
    count_++;
  }
  std::vector<Value> num_of_rows;
  num_of_rows.emplace_back(INTEGER, count_);
  *tuple = Tuple(num_of_rows, &GetOutputSchema());
  return true;
}

}  // namespace bustub
