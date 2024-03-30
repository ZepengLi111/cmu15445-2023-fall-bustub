//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::vector<std::pair<Tuple, RID>> tuples_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<std::pair<Tuple, RID>>::iterator it_;
};

class CompareTuple {
 public:
  CompareTuple(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> *order_bys, const Schema *schema) {
    order_bys_ = order_bys;
    schema_ = schema;
  }

  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> *order_bys_;
  const Schema *schema_;

  auto operator()(std::pair<Tuple, RID> &a, std::pair<Tuple, RID> &b) const -> bool {
    for (const auto &order_by : (*order_bys_)) {
      auto value_a = order_by.second->Evaluate(&(a.first), *schema_);
      auto value_b = order_by.second->Evaluate(&(b.first), *schema_);
      if (order_by.first == OrderByType::DESC) {
        if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
          return true;
        }
        if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
          return false;
        }
      } else if (order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT) {
        if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
          return false;
        }
        if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
          return true;
        }
      }
    }
    return true;
  }
};

}  // namespace bustub
