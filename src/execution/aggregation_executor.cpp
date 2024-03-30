//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());
}

void AggregationExecutor::Init() {
  aht_->Clear();
  tuples_.clear();
  child_executor_->Init();
  Tuple temp_tuple;
  RID temp_rid;
  while (child_executor_->Next(&temp_tuple, &temp_rid)) {
    tuples_.emplace_back(temp_tuple);
  }
  for (auto &tuple : tuples_) {
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    aht_->InsertCombine(key, value);
  }
  if (tuples_.empty() && plan_->GetGroupBys().empty()) {
    aht_->InitForEmpty();
  }
  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*aht_iterator_ == aht_->End()) {
    return false;
  }
  std::vector<Value> tuple_values = aht_iterator_->Key().group_bys_;
  tuple_values.insert(tuple_values.end(), aht_iterator_->Val().aggregates_.begin(),
                      aht_iterator_->Val().aggregates_.end());
  *tuple = Tuple(tuple_values, &GetOutputSchema());
  ++(*aht_iterator_);
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
