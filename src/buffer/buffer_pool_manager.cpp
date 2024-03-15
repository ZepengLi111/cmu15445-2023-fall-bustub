//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::GetNewFrameId(frame_id_t *frame_id) -> bool {
  frame_id_t new_frame_id = -1;
  page_id_t page_id = -1;
  // 先从free_list中找
  if (!free_list_.empty()) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  }
  // 否则就驱逐已有的页
  else {
    replacer_->Evict(&new_frame_id);
  }
  if (new_frame_id == -1) {
    return false;
  }
  // 找到页帧
  Page &page = pages_[new_frame_id];
  page_id = page.page_id_;
  //  page.WLatch();
  if (page.IsDirty()) {
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule({true, page.GetData(), page.GetPageId(), std::move(promise1)});
    if (!future1.get()) {
      // log
    }
  }
  //  std::unique_lock lock_p{latch_page_table_};
  page_table_.erase(page_id);
  //  lock_p.unlock();
  page.pin_count_ = 0;
  page.page_id_ = INVALID_PAGE_ID;
  page.is_dirty_ = false;
  page.ResetMemory();
  //  page.WUnlatch();
  *frame_id = new_frame_id;
  return true;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock lock{latch_};
  page_id_t new_page_id = AllocatePage();
  frame_id_t new_frame_id;
  auto get_new_frame_success = GetNewFrameId(&new_frame_id);
  if (!get_new_frame_success) {
    return nullptr;
  }
  Page &page = pages_[new_frame_id];
  //  page.WLatch();
  //  std::unique_lock lock_p{latch_page_table_};
  page_table_.erase(new_page_id);
  page_table_.insert(std::make_pair(new_page_id, new_frame_id));
  //  lock_p.unlock();
  page.pin_count_ += 1;
  page.page_id_ = new_page_id;
  //  page.WUnlatch();
  // 标记页访问
  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);
  *page_id = new_page_id;
  return &page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // 查看页是否在内存中
  std::unique_lock lock{latch_};
  //  std::shared_lock lock_p{latch_page_table_};
  auto frame_iter = page_table_.find(page_id);
  if (frame_iter != page_table_.end()) {
    // 页在内存中
    Page &page = pages_[frame_iter->second];
    page.pin_count_ += 1;
    replacer_->RecordAccess(frame_iter->second);
    replacer_->SetEvictable(frame_iter->second, false);
    return &page;
  }
  //  lock_p.unlock();
  // 页不在内存中
  frame_id_t new_frame_id;
  auto get_new_frame_success = GetNewFrameId(&new_frame_id);
  if (!get_new_frame_success) {
    return nullptr;
  }
  Page &page = pages_[new_frame_id];
  //  page.WLatch();
  //  std::unique_lock lock{latch_page_table_};
  page_table_.insert(std::make_pair(page_id, new_frame_id));
  //  lock.unlock();
  page.page_id_ = page_id;
  page.pin_count_ += 1;
  auto promise1 = disk_scheduler_->CreatePromise();
  auto future1 = promise1.get_future();
  disk_scheduler_->Schedule({false, page.GetData(), page.GetPageId(), std::move(promise1)});
  if (!future1.get()) {
    // log
  }
  //  page.WUnlatch();
  // 标记页访问
  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);
  return &page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock lock{latch_};
  //  std::shared_lock lock_p{latch_page_table_};
  auto frame_id_iter = page_table_.find(page_id);
  if (frame_id_iter == page_table_.end()) {
    return false;
  }
  Page &page = pages_[frame_id_iter->second];
  //  lock_p.unlock();
  //  page.RLatch();
  if (page.pin_count_ <= 0) {
    return false;
  }
  page.pin_count_ -= 1;
  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id_iter->second, true);
  }
  if (is_dirty) {
    page.is_dirty_ = true;
  }
  //  page.RUnlatch();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // 寻找内存中是否有该页
  std::unique_lock lock{latch_};
  //  std::shared_lock lock_p{latch_page_table_};
  auto frame_id_iter = page_table_.find(page_id);
  if (frame_id_iter == page_table_.end()) {
    return false;
  }
  //  lock_p.unlock();
  Page &page = pages_[frame_id_iter->second];
  // 向磁盘中写入，只需要加读锁
  //  page.WLatch();
  auto promise1 = disk_scheduler_->CreatePromise();
  auto future1 = promise1.get_future();
  disk_scheduler_->Schedule({true, page.GetData(), page.GetPageId(), std::move(promise1)});
  auto res = future1.get();
  if (!res) {
    // log
  }
  //  BUSTUB_ASSERT(future1.get(), "page writing failed");
  page.is_dirty_ = false;
  //  page.WUnlatch();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock lock{latch_};
  //  std::shared_lock lock_p{latch_page_table_};
  for (auto page_table_entry : page_table_) {
    auto res = FlushPage(page_table_entry.first);
    //    BUSTUB_ASSERT(res, "flush page failed ");
    if (!res) {
      // log
    }
  }
  //  lock_p.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // 寻找内存中是否有该页
  std::unique_lock lock{latch_};
  //  std::shared_lock lock_p{latch_page_table_};
  auto frame_id_iter = page_table_.find(page_id);
  if (frame_id_iter == page_table_.end()) {
    return true;
  }
  //  lock_p.unlock();
  Page &page = pages_[frame_id_iter->second];
  //  page.WLatch();
  if (page.GetPinCount() > 0) {
    return false;
  }
  replacer_->Remove(frame_id_iter->second);
  free_list_.emplace_back(frame_id_iter->second);
  page.pin_count_ = 0;
  page.page_id_ = INVALID_PAGE_ID;
  page.ResetMemory();
  page.is_dirty_ = false;
  //  page.WUnlatch();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
