#include "concurrency/watermark.h"
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  // TODO(fall2023): implement me!
  current_reads_[read_ts] += 1;
  current_reads_heap_.push(read_ts);
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  current_reads_[read_ts] -= 1;
  if (current_reads_[watermark_] == 0) {
    while (!current_reads_heap_.empty() && current_reads_[current_reads_heap_.top()] == 0) {
      current_reads_heap_.pop();
    }
    if (current_reads_heap_.empty()) {
      watermark_ = commit_ts_;
    } else {
      watermark_ = current_reads_heap_.top();
    }
  }
}

}  // namespace bustub
