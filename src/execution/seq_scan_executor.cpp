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
  TupleMeta meta{};
  Tuple temp_tuple;
  auto filter_expr = plan_->filter_predicate_;
  while (!iter_->IsEnd()) {
    auto tuple_temp = iter_->GetTuple();
    RID rid1 = iter_->GetRID();
    meta = tuple_temp.first;
    temp_tuple = tuple_temp.second;
    ++(*iter_);

    // case 1：要读的数据时间戳小于等于当前的读时间戳，不需要改变tuple。
    // case 2：要读的时间戳与transaction temporary timestamp相等，也不需要改变。
    if (meta.ts_ <= exec_ctx_->GetTransaction()->GetReadTs() ||
        meta.ts_ == exec_ctx_->GetTransaction()->GetTransactionTempTs()) {
      if (meta.is_deleted_) {
        continue;
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
          continue;
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
          continue;
        }
      }
      auto reconstructed_tuple = ReconstructTuple(&GetOutputSchema(), temp_tuple, meta, undo_logs);
      if (!reconstructed_tuple.has_value()) {
        continue;
      }
      temp_tuple = reconstructed_tuple.value();
    }
    if (filter_expr) {
      auto value = filter_expr->Evaluate(&temp_tuple, GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        continue;
      }
    }
    temp_tuple.SetRid(rid1);
    *tuple = temp_tuple;
    *rid = temp_tuple.GetRid();
    std::stringstream ss1;
    ss1 << std::this_thread::get_id();
    //    fmt::println(stderr, "SEQ SCAN tid={} RID={}/{} RID={}/{}", ss1.str(), rid->GetPageId(), rid->GetSlotNum(),
    //    temp_tuple.GetRid().GetPageId(), temp_tuple.GetRid().GetPageId());
    return true;
  }
  return false;
}

}  // namespace bustub
