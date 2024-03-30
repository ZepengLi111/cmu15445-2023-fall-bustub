#include "execution/executors/window_function_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  std::vector<std::pair<Tuple, RID>> tuples;

  tuples_.clear();
  child_executor_->Init();
  Tuple temp_tuple;
  RID temp_rid;
  auto has_next = child_executor_->Next(&temp_tuple, &temp_rid);
  while (has_next) {
    tuples.emplace_back(std::make_pair(temp_tuple, temp_rid));
    has_next = child_executor_->Next(&temp_tuple, &temp_rid);
  }

  // 先检测是否存在order by，有则先排序
  for (unsigned i = 0; i < plan_->columns_.size(); i++) {
    auto it = plan_->window_functions_.find(i);
    if (it != plan_->window_functions_.end()) {
      if (!it->second.order_by_.empty()) {
        std::sort(tuples.begin(), tuples.end(),
                  CompareTuple(&it->second.order_by_, &child_executor_->GetOutputSchema()));
        having_order_by_ = true;
        break;
      }
    }
  }

  std::vector<Value> values;
  Tuple last_tuple = tuples[0].first;
  Value last_rank = Value(INTEGER, 1);
  for (const auto &tuple_rid : tuples) {
    values.clear();
    for (unsigned i = 0; i < plan_->columns_.size(); i++) {
      auto it = plan_->window_functions_.find(i);
      if (it != plan_->window_functions_.end()) {
        // 该列是窗口函数
        Value value = MakeValue(&tuple_rid.first, it->second.function_);
        AggregateKey key = MakeAggregateKey(&tuple_rid.first, it->second.partition_by_);
        window_map_.InsertCombine(key, value, it->second.type_);
        if (having_order_by_) {
          if (it->second.type_ == WindowFunctionType::Rank) {
            // 假设order by只有一列
            auto last_order_by =
                it->second.order_by_[0].second->Evaluate(&last_tuple, child_executor_->GetOutputSchema());
            auto this_order_by =
                it->second.order_by_[0].second->Evaluate(&tuple_rid.first, child_executor_->GetOutputSchema());

            if (last_order_by.CompareEquals(this_order_by) == CmpBool::CmpTrue) {
              values.push_back(last_rank);
            } else {
              last_rank = window_map_.GetValueFromMap(key, it->second.type_);
              values.push_back(last_rank);
            }
          } else {
            values.push_back(window_map_.GetValueFromMap(key, it->second.type_));
          }
        }
      } else {
        if (having_order_by_) {
          values.push_back(plan_->columns_[i]->Evaluate(&tuple_rid.first, child_executor_->GetOutputSchema()));
        }
      }
    }
    if (having_order_by_) {
      tuples_.emplace_back(std::make_pair(Tuple(values, &GetOutputSchema()), tuple_rid.second));
    }
    last_tuple = tuple_rid.first;
  }

  if (!having_order_by_) {
    std::vector<Value> values;
    for (const auto &tuple_rid : tuples) {
      values.clear();
      for (unsigned i = 0; i < plan_->columns_.size(); i++) {
        auto it = plan_->window_functions_.find(i);
        if (it != plan_->window_functions_.end()) {
          // 该列是窗口函数
          AggregateKey key = MakeAggregateKey(&tuple_rid.first, it->second.partition_by_);
          values.push_back(window_map_.GetValueFromMap(key, it->second.type_));
        } else {
          values.push_back(plan_->columns_[i]->Evaluate(&tuple_rid.first, child_executor_->GetOutputSchema()));
        }
      }
      tuples_.emplace_back(std::make_pair(Tuple(values, &GetOutputSchema()), tuple_rid.second));
    }
  }
  it_ = tuples_.begin();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == tuples_.end()) {
    return false;
  }

  *tuple = it_->first;
  *rid = it_->second;
  it_++;
  return true;
}
}  // namespace bustub
