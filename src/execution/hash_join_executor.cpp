//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
  }
}

void HashJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  ht_.clear();
  candidate_tuples_.clear();
  Tuple left_tuple;
  RID temp_rid;
  auto has_left_next = left_child_executor_->Next(&left_tuple, &temp_rid);
  while (has_left_next) {
    HashJoinKey key =
        MakeHashJoinKey(&left_tuple, plan_->LeftJoinKeyExpressions(), left_child_executor_->GetOutputSchema());
    ht_[key].tuples_.emplace_back(left_tuple);
    has_left_next = left_child_executor_->Next(&left_tuple, &temp_rid);
  }
  it_ = ht_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!candidate_tuples_.empty()) {
    *tuple = candidate_tuples_.back();
    candidate_tuples_.pop_back();
    return true;
  }

  Tuple right_tuple;
  RID temp_rid;
  auto has_right_next = right_child_executor_->Next(&right_tuple, &temp_rid);
  while (has_right_next) {
    HashJoinKey key =
        MakeHashJoinKey(&right_tuple, plan_->RightJoinKeyExpressions(), right_child_executor_->GetOutputSchema());
    if (ht_.find(key) != ht_.end()) {
      for (const auto &left_tuple : ht_[key].tuples_) {
        std::vector<Value> out_values;
        for (unsigned i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumns().size(); i++) {
          out_values.emplace_back(left_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
        }
        for (unsigned i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumns().size(); i++) {
          out_values.emplace_back(right_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
        }
        candidate_tuples_.emplace_back(Tuple(out_values, &GetOutputSchema()));
      }
      ht_[key].is_joined_ = true;
      *tuple = candidate_tuples_.back();
      candidate_tuples_.pop_back();
      return true;
    }
    has_right_next = right_child_executor_->Next(&right_tuple, &temp_rid);
  }
  if (plan_->GetJoinType() == JoinType::LEFT) {
    while (it_ != ht_.end()) {
      if (!it_->second.is_joined_) {
        for (const auto &left_tuple : it_->second.tuples_) {
          std::vector<Value> out_values;
          for (unsigned i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumns().size(); i++) {
            out_values.emplace_back(left_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
          }
          for (unsigned i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumns().size(); i++) {
            out_values.emplace_back(
                ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
          }
          candidate_tuples_.emplace_back(Tuple(out_values, &GetOutputSchema()));
        }
        it_->second.is_joined_ = true;
        *tuple = candidate_tuples_.back();
        candidate_tuples_.pop_back();
        return true;
      }
      it_++;
    }
  }
  return false;
}

}  // namespace bustub
