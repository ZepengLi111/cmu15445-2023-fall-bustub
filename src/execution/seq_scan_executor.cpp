//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_->IsEnd()) {
    return false;
  }
  TupleMeta meta;
  Tuple temp_tuple;
  auto filter_expr = plan_->filter_predicate_;
  while (!iter_->IsEnd()) {
    auto tuple_temp = iter_->GetTuple();
    meta = tuple_temp.first;
    temp_tuple = tuple_temp.second;
    ++(*iter_);
    if (meta.is_deleted_) {
      continue;
    }
    if (filter_expr) {
      auto value = filter_expr->Evaluate(&temp_tuple, GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        *tuple = temp_tuple;
        *rid = temp_tuple.GetRid();
        return true;
      }
    } else {
      *tuple = temp_tuple;
      *rid = temp_tuple.GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
