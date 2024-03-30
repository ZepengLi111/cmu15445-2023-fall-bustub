#include "execution/executors/topn_executor.h"
#include "execution/executors/sort_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  if (tuples_.empty()) {
    child_executor_->Init();
    Tuple temp_tuple;
    RID temp_rid;
    CompareTuple comp(&plan_->GetOrderBy(), &child_executor_->GetOutputSchema());

    std::priority_queue<std::pair<Tuple, RID>, std::vector<std::pair<Tuple, RID>>, CompareTuple> topn_priority_queue(
        comp);

    bool has_next = child_executor_->Next(&temp_tuple, &temp_rid);
    auto tuple_rid = std::make_pair(temp_tuple, temp_rid);
    while (has_next) {
      if (topn_priority_queue.size() >= plan_->GetN()) {
        std::pair<Tuple, RID> top_tuple_rid = topn_priority_queue.top();
        if (comp(tuple_rid, top_tuple_rid)) {
          topn_priority_queue.pop();
          topn_priority_queue.push(tuple_rid);
        }
      } else {
        topn_priority_queue.push(tuple_rid);
      }
      has_next = child_executor_->Next(&temp_tuple, &temp_rid);
      tuple_rid = std::make_pair(temp_tuple, temp_rid);
    }
    unsigned count = plan_->GetN() > topn_priority_queue.size() ? topn_priority_queue.size() : plan_->GetN();
    for (unsigned int i = 0; i < count; i++) {
      tuples_.emplace_back(topn_priority_queue.top());
      topn_priority_queue.pop();
    }
  }
  it_ = tuples_.rbegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == tuples_.rend()) {
    return false;
  }
  *tuple = it_->first;
  *rid = it_->second;
  it_++;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return tuples_.size(); };

}  // namespace bustub
