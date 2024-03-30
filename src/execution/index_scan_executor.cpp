//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  h_table_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(
      exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_.get());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }
  is_finished_ = true;

  std::vector<RID> result;
  std::vector<Value> values;
  values.push_back(plan_->pred_key_->val_);
  Schema schema = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->key_schema_;
  //  Schema schema(std::vector<Column>{(" ", plan_->pred_key_->val_.GetTypeId())});
  Tuple tuple1(values, &schema);
  h_table_->ScanKey(tuple1, &result, exec_ctx_->GetTransaction());
  if (result.empty()) {
    return false;
  }

  auto temp_rid = result[0];
  auto [meta_, tuple_] = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(temp_rid);
  if (meta_.is_deleted_) {
    return false;
  }
  if (plan_->filter_predicate_) {
    auto value = plan_->filter_predicate_->Evaluate(&tuple_, GetOutputSchema());
    if (value.IsNull() || !value.GetAs<bool>()) {
      return false;
    }
  }
  *tuple = tuple_;
  *rid = tuple_.GetRid();
  return true;
}

}  // namespace bustub
