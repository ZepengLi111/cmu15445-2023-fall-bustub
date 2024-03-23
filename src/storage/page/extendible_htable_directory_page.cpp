//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  max_depth_ = max_depth;
  global_depth_ = 0;
  //  for (uint32_t i = 0; i < (1 << max_depth); i++)
  //  {
  //    bucket_page_ids_[i] = INVALID_PAGE_ID;
  //  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  if (global_depth_ == 0) {
    return 0;
  }
  return hash << (sizeof(uint32_t) * 8 - global_depth_) >> (sizeof(uint32_t) * 8 - global_depth_);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
  //  return INVALID_PAGE_ID;
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx, uint32_t depth) const -> uint32_t {
  if (depth == 0) {
    return bucket_idx;
  }
  uint32_t local_size = (1 << (depth - 1));
  if (bucket_idx < local_size) {
    return bucket_idx + local_size;
  }
  return bucket_idx - local_size;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  BUSTUB_ASSERT(global_depth_ + 1 <= max_depth_, "cannot increase global depth ");
  for (uint32_t i = 0; i < Size(); i++) {
    local_depths_[i + Size()] = local_depths_[i];
    bucket_page_ids_[i + Size()] = bucket_page_ids_[i];
  }
  global_depth_ += 1;
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  BUSTUB_ASSERT(global_depth_ >= 1, "cannot decrease global depth ");
  global_depth_ -= 1;
  //  for (uint32_t i = Size(); i < Size()*2; i++)
  //  {
  //
  //  }
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint32_t i = 0; i < Size(); i++) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket id is greater that size()");
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket id is greater that size()");
  local_depths_[bucket_idx] += 1;
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket id is greater that size()");
  BUSTUB_ASSERT(local_depths_[bucket_idx] >= 1, "cannot decrease depth below 1 ");
  local_depths_[bucket_idx] -= 1;
}
auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

}  // namespace bustub
