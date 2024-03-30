//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  is_finished_ = false;
  has_found_for_left_tuple_ = false;
  RID temp_rid;
  auto has_left_next = left_executor_->Next(&left_tuple_, &temp_rid);
  if (!has_left_next) {
    is_finished_ = true;
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }
  RID temp_rid;
  Tuple right_tuple;
  while (true) {
    while (true) {
      auto has_right = right_executor_->Next(&right_tuple, &temp_rid);
      if (!has_right) {
        break;
      }
      auto value = plan_->Predicate()->EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                                                    plan_->GetRightPlan()->OutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        has_found_for_left_tuple_ = true;
        std::vector<Value> out_values;
        for (unsigned i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumns().size(); i++) {
          out_values.emplace_back(left_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
        }
        for (unsigned i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumns().size(); i++) {
          out_values.emplace_back(right_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
        }
        *tuple = Tuple(out_values, &GetOutputSchema());
        return true;
      }
    }
    if (plan_->GetJoinType() == JoinType::LEFT && !has_found_for_left_tuple_) {
      std::vector<Value> out_values;
      for (unsigned i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumns().size(); i++) {
        out_values.emplace_back(left_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
      }
      for (unsigned i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumns().size(); i++) {
        out_values.emplace_back(
            ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(out_values, &GetOutputSchema());
      has_found_for_left_tuple_ = true;
      return true;
    }
    auto has_left_next = left_executor_->Next(&left_tuple_, &temp_rid);
    if (!has_left_next) {
      is_finished_ = true;
      return false;
    }
    right_executor_->Init();
    has_found_for_left_tuple_ = false;
  }
}

}  // namespace bustub
