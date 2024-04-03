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
  is_finished_= false;
  h_table_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(
      exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_.get());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  fmt::println(stderr, "INDEX SCAN RID={}/{}", rid->GetPageId(), rid->GetSlotNum());
  if (is_finished_) {
    return false;
  }
  is_finished_ = true;

  std::vector<RID> result;
  std::vector<Value> values;
  values.push_back(plan_->pred_key_->val_);
  Schema schema = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->key_schema_;
  Tuple tuple1(values, &schema);
  h_table_->ScanKey(tuple1, &result, exec_ctx_->GetTransaction());
  if (result.empty()) {
    return false;
  }
  auto temp_rid = result[0];
  auto [temp_meta, temp_tuple] = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(temp_rid);

  // case 1：要读的数据时间戳小于等于当前的读时间戳，不需要改变tuple。
  // case 2：要读的时间戳与transaction temporary timestamp相等，也不需要改变。
  if (temp_meta.ts_ <= exec_ctx_->GetTransaction()->GetReadTs() ||
      temp_meta.ts_ == exec_ctx_->GetTransaction()->GetTransactionTempTs()) {
    if (temp_meta.is_deleted_) {
      // 要读的已经被删除掉了，返回false
      return false;
    }
  }
  // case 3；要读的时间戳大于当前的读时间戳，需要使用undo日志改变tuple
  else {
    std::vector<UndoLog> undo_logs;
    auto undo_link_opt = exec_ctx_->GetTransactionManager()->GetUndoLink(temp_tuple.GetRid());
    if (undo_link_opt.has_value()) {
      auto undo_link = undo_link_opt.value();
      if (!undo_link.IsValid()) {
        // 如果第一个undo link就无效，在case3中可以直接continue
        return false;
      }
      auto undo_log = exec_ctx_->GetTransactionManager()->GetUndoLogOptional(undo_link);
      while (undo_log.has_value()) {
        undo_logs.push_back(undo_log.value());
        if (!undo_log->prev_version_.IsValid() || undo_log->ts_ <= exec_ctx_->GetTransaction()->GetReadTs()) {
          break;
        }
        undo_log = exec_ctx_->GetTransactionManager()->GetUndoLogOptional(undo_log->prev_version_);
      }
      if (undo_log.has_value() && undo_log->ts_ > exec_ctx_->GetTransaction()->GetReadTs()) {
        // 在情况三中，如果遍历了所有undo日志，发现最终的ts仍然比read_ts大，则代表这个tuple不可读
        return false;
      }
    }
    auto reconstructed_tuple = ReconstructTuple(&GetOutputSchema(), temp_tuple, temp_meta, undo_logs);
    if (!reconstructed_tuple.has_value()) {
      // 构造失败，说明要读的是被删除的tuple，返回false
      return false;
    }
    temp_tuple = reconstructed_tuple.value();
  }

  if (plan_->filter_predicate_) {
    auto value = plan_->filter_predicate_->Evaluate(&temp_tuple, GetOutputSchema());
    if (value.IsNull() || !value.GetAs<bool>()) {
      return false;
    }
  }

  temp_tuple.SetRid(temp_rid);
  *tuple = temp_tuple;
  *rid = temp_rid;

  return true;
}

}  // namespace bustub
