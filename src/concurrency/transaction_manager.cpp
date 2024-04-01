//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = ++next_txn_id_;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  timestamp_t c_ts = last_commit_ts_ + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  for (const auto &[t_oid, rids] : txn->write_set_) {
    for (const auto &rid : rids) {
      auto [meta, tuple] = catalog_->GetTable(t_oid)->table_->GetTuple(rid);
      catalog_->GetTable(t_oid)->table_->UpdateTupleMeta({c_ts, meta.is_deleted_}, rid);
    }
  }
  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = c_ts;
  last_commit_ts_ = c_ts;
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  std::unordered_map<txn_id_t, unsigned> txn_invisible_log_num;

  auto table_names = catalog_->GetTableNames();
  for (const auto &table_name : table_names) {
    auto table_info = catalog_->GetTable(table_name);
    auto it = table_info->table_->MakeIterator();
    while (!it.IsEnd()) {
      bool should_be_deleted = false;
      if (table_info->table_->GetTupleMeta(it.GetRID()).ts_ <= running_txns_.watermark_) {
        should_be_deleted = true;
      }
      auto undo_link = GetUndoLink(it.GetRID());
      while (undo_link.has_value() && undo_link->IsValid()) {
        auto undo_log = GetUndoLogOptional(undo_link.value());
        if (!undo_log.has_value()) {
          break;
        }
        if (should_be_deleted) {
          txn_invisible_log_num[undo_link->prev_txn_] += 1;
        }
        if (undo_log->ts_ <= running_txns_.watermark_) {
          should_be_deleted = true;
        }
        undo_link = undo_log->prev_version_;
      }
      ++it;
    }
  }
  std::vector<txn_id_t> delete_ids;
  for (const auto &pair : txn_map_) {
    if (txn_invisible_log_num[pair.second->txn_id_] == pair.second->GetUndoLogNum() &&
        (pair.second->state_ == TransactionState::COMMITTED || pair.second->state_ == TransactionState::ABORTED)) {
      pair.second->ClearUndoLog();
      delete_ids.push_back(pair.first);
    }
  }
  for (auto id : delete_ids) {
    txn_map_.erase(id);
  }
}

}  // namespace bustub
