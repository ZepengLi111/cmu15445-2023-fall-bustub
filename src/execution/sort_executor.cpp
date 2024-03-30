#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  child_executor_->Init();
  Tuple temp_tuple;
  RID rid;
  auto having_next = child_executor_->Next(&temp_tuple, &rid);
  while (having_next) {
    tuples_.emplace_back(std::make_pair(temp_tuple, rid));
    having_next = child_executor_->Next(&temp_tuple, &rid);
  }
  std::sort(tuples_.begin(), tuples_.end(), CompareTuple(&plan_->GetOrderBy(), &child_executor_->GetOutputSchema()));
}

void SortExecutor::Init() { it_ = tuples_.begin(); }

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ != tuples_.end()) {
    *tuple = it_->first;
    *rid = it_->second;
    it_++;
    return true;
  }
  return false;
}

}  // namespace bustub
