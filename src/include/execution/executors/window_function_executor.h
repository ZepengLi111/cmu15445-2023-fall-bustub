//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

#define WINDOW_TYPE_NUM 6

namespace bustub {

class WindowMap {
 public:
  auto MapWindowTypeToInteger(WindowFunctionType type) -> int {
    int value = 0;
    switch (type) {
      case WindowFunctionType::CountStarAggregate:
        value = 0;
        break;
      case WindowFunctionType::CountAggregate:
        value = 1;
        break;
      case WindowFunctionType::SumAggregate:
        value = 2;
        break;
      case WindowFunctionType::MinAggregate:
        value = 3;
        break;
      case WindowFunctionType::MaxAggregate:
        value = 4;
        break;
      case WindowFunctionType::Rank:
        value = 5;
        break;
    }
    return value;
  }

  auto GenerateInitialPartitionValue() -> AggregateValue {
    // 默认包含所有种类的窗口函数
    std::vector<Value> values;
    values.push_back(ValueFactory::GetIntegerValue(0));
    values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
    values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
    values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
    values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
    values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
    return {values};
  }

  void InsertCombine(const AggregateKey &key, const Value &value, WindowFunctionType type) {
    if (window_map_.count(key) == 0) {
      window_map_.insert({key, GenerateInitialPartitionValue()});
    }
    CombineAggregateValues(&window_map_[key], value, type);
  }

  void CombineAggregateValues(AggregateValue *result, const Value &input, WindowFunctionType type) {
    int type_id = MapWindowTypeToInteger(type);
    switch (type) {
      case WindowFunctionType::CountStarAggregate:
        result->aggregates_[type_id] = result->aggregates_[type_id].Add(Value(INTEGER, 1));
        break;
      case WindowFunctionType::CountAggregate:
        if (!input.IsNull()) {
          if (result->aggregates_[type_id].IsNull()) {
            result->aggregates_[type_id] = Value(INTEGER, 0);
          }
          result->aggregates_[type_id] = result->aggregates_[type_id].Add(Value(INTEGER, 1));
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (!input.IsNull()) {
          if (result->aggregates_[type_id].IsNull()) {
            result->aggregates_[type_id] = Value(INTEGER, 0);
          }
          result->aggregates_[type_id] = result->aggregates_[type_id].Add(input);
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (!input.IsNull()) {
          if (result->aggregates_[type_id].IsNull()) {
            result->aggregates_[type_id] = input;
          } else {
            result->aggregates_[type_id] = result->aggregates_[type_id].Min(input);
          }
        }
        break;

      case WindowFunctionType::MaxAggregate:
        if (!input.IsNull()) {
          if (result->aggregates_[type_id].IsNull()) {
            result->aggregates_[type_id] = input;
          } else {
            result->aggregates_[type_id] = result->aggregates_[type_id].Max(input);
          }
        }
        break;
      case WindowFunctionType::Rank:
        if (!input.IsNull()) {
          if (result->aggregates_[type_id].IsNull()) {
            result->aggregates_[type_id] = Value(INTEGER, 0);
          }
          result->aggregates_[type_id] = result->aggregates_[type_id].Add(Value(INTEGER, 1));
        }
        break;
    }
  }

  auto GetValueFromMap(AggregateKey &key, WindowFunctionType type) -> Value {
    return window_map_[key].aggregates_[MapWindowTypeToInteger(type)];
  }

 private:
  std::unordered_map<AggregateKey, AggregateValue> window_map_;
};

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  auto MakeAggregateKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &partition_bys) -> AggregateKey {
    std::vector<Value> keys;
    keys.reserve(partition_bys.size());
    for (const auto &expr : partition_bys) {
      keys.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  auto MakeValue(const Tuple *tuple, const AbstractExpressionRef &function) -> Value {
    Value value = function->Evaluate(tuple, child_executor_->GetOutputSchema());
    return value;
  }

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<std::pair<Tuple, RID>> tuples_;
  std::vector<std::pair<Tuple, RID>>::iterator it_;
  WindowMap window_map_;
  bool having_order_by_ = false;
  //  std::vector<std::pair<Tuple, RID>> tuples_;
};

}  // namespace bustub
