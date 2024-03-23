//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  //  std::cout << "new hash table header_max_depth: " << header_max_depth << ", directory_max_depth："
  //            << directory_max_depth << ", bucket_max_size：" << bucket_max_size << ", hash_table_name: " << name
  //            << std::endl;
  //  std::cout << "k size : " << sizeof(K) << " v size: " << sizeof(V) << std::endl;
  //  std::cout << "pool size : " << bpm_->GetPoolSize() << std::endl;
  index_name_ = name;
  page_id_t header_page_id = INVALID_PAGE_ID;
  BasicPageGuard header_guard = bpm->NewPageGuarded(&header_page_id);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
  header_page_id_ = header_guard.PageId();
  header_guard.Drop();
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  //  std::cout << "hash table--" << index_name_ << " get value key: " << key << std::endl;
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  auto hash_value = Hash(key);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash_value));
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // 提前释放
  auto directory_page_guard = bpm_->FetchPageRead(directory_page_id);
  header_guard.Drop();
  auto directory_page = directory_page_guard.As<ExtendibleHTableDirectoryPage>();
  page_id_t bucket_page_id = directory_page->GetBucketPageId(directory_page->HashToBucketIndex(hash_value));
  // 提前释放
  auto bucket_page_guard = bpm_->FetchPageRead(bucket_page_id);
  directory_page_guard.Drop();
  auto bucket_page = bucket_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  auto look_success = bucket_page->Lookup(key, value, cmp_);
  if (look_success) {
    (*result).push_back(value);
  }
  return look_success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  //  std::cout << " insert : " << key;
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto hash_value = Hash(key);
  //  LOG_INFO("insert hash value %u", hash_value);
  page_id_t directory_index_id = header_page->HashToDirectoryIndex(hash_value);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_index_id);

  WritePageGuard w_directory_guard;
  // step 1-a: 如果目录页不存在，则创建新的目录页
  if (directory_page_id == INVALID_PAGE_ID) {
    BasicPageGuard directory_guard = bpm_->NewPageGuarded(&directory_page_id);
    w_directory_guard = directory_guard.UpgradeWrite();
    auto directory_page = w_directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    header_page->SetDirectoryPageId(header_page->HashToDirectoryIndex(hash_value), directory_page_id);
    directory_page->Init(directory_max_depth_);
    return InsertToNewDirectory(directory_page, key, value);
  }
  header_guard.Drop();
  // step 1-b: 目录页存在，找到即可
  w_directory_guard = bpm_->FetchPageWrite(directory_page_id);

  // step 2：找到桶页，一定是可以找到的
  auto directory_page = w_directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_id = directory_page->HashToBucketIndex(hash_value);
  //  LOG_INFO("insert bucket id %d", directory_page->HashToBucketIndex(hash_value));
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_id);
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  // step 3-a：如果桶满了，需要扩大目录页，创建新的桶
  while (bucket_page->IsFull()) {
    page_id_t new_bucket_page_id;
    WritePageGuard new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
    auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);
    if (directory_page->GetLocalDepth(bucket_id) == directory_page->GetGlobalDepth()) {
      // step 3-a-a：如果局部深度和全局深度一样大，需要扩大全局深度
      if (directory_page->GetGlobalDepth() == directory_page->GetMaxDepth()) {
        // 如果全局深度已经是最大深度了，则无法再扩充了
        return false;
      }
      directory_page->IncrGlobalDepth();
    }
    // step 3-a-b：将新的桶放入目录中，并且修改相应的id，需要使用循环。e.g.
    // 如果global_depth有4，总共有16个元素。所有的偶数桶(0,2,4,6,8,...)都属于
    // 同一个桶。如果此时要分裂，新桶不只是要更新到2，还要更新到6，10。
    auto now_depth = directory_page->GetLocalDepth(bucket_id) + 1;
    auto skip_entry_num = 1 << now_depth;
    // 获取将要分隔的桶的ID
    auto new_bucket_id = directory_page->GetSplitImageIndex(bucket_id, directory_page->GetLocalDepth(bucket_id) + 1);
    int loop_count = 0;
    while (bucket_id + skip_entry_num * loop_count < directory_page->Size() &&
           new_bucket_id + skip_entry_num * loop_count < directory_page->Size()) {
      directory_page->SetBucketPageId(new_bucket_id + loop_count * skip_entry_num, new_bucket_page_id);
      directory_page->IncrLocalDepth(bucket_id + loop_count * skip_entry_num);
      directory_page->IncrLocalDepth(new_bucket_id + loop_count * skip_entry_num);
      loop_count++;
    }
    SplitToNewBucket(new_bucket_page, new_bucket_id, bucket_page, directory_page);

    // step 3-a-c：查看要插入的kv所属于的桶，更新bucket_id和bucket_page
    if (directory_page->HashToBucketIndex(hash_value) == new_bucket_id) {
      // 属于新建的桶，需要更新指针
      bucket_guard.Drop();
      bucket_guard = std::move(new_bucket_guard);
      bucket_page = new_bucket_page;
      bucket_id = new_bucket_id;
      bucket_page_id = new_bucket_page_id;
    }
    // 属于当前桶，则不需要更新指针
  }
  // step 3-b：查看桶内是否有该键，如果没有则插入，如果有返回false
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableDirectoryPage *directory, const K &key,
                                                             const V &value) -> bool {
  page_id_t bucket_page_id;
  BasicPageGuard bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
  WritePageGuard w_bucket_guard = bucket_guard.UpgradeWrite();
  auto bucket_page = w_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(0, bucket_page_id);
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory,
                                                          ExtendibleHTableBucketPage<K, V, KC> *bucket, const K &key,
                                                          const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::SplitToNewBucket(ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                         uint32_t new_bucket_id,
                                                         ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                         ExtendibleHTableDirectoryPage *directory_page) {
  // 将旧桶的键值重新分配到新的桶中
  std::vector<uint32_t> remove_vec;
  for (uint32_t i = 0; i < old_bucket->Size(); i++) {
    auto key = old_bucket->KeyAt(i);
    auto hash = Hash(key);
    auto index = directory_page->HashToBucketIndex(hash);
    if (index == new_bucket_id) {
      auto entry = old_bucket->EntryAt(i);
      remove_vec.push_back(i);
      new_bucket->Insert(entry.first, entry.second, cmp_);
    }
  }
  //  for (unsigned int &i : remove_vec) {
  //    old_bucket->RemoveAt(i);
  //  }
  for (auto it = remove_vec.rbegin(); it != remove_vec.rend(); ++it) {
    old_bucket->RemoveAt(*it);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  //  std::cout << " remove " << key;
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  auto hash_value = Hash(key);
  //  LOG_INFO("remove hash value %u", hash_value);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash_value));
  header_guard.Drop();
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  page_id_t bucket_id = directory_page->HashToBucketIndex(hash_value);
  //  LOG_INFO("remove bucket id %d", bucket_id);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_id);
  //  std::cout << "page id: " << bucket_page_id << std::endl;
  auto bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
  //  directory_page_guard.Drop();
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  auto remove_success = bucket_page->Remove(key, cmp_);
  if (!remove_success) {
    return false;
  }

  page_id_t image_bucket_id = directory_page->GetSplitImageIndex(bucket_id, directory_page->GetLocalDepth(bucket_id));
  page_id_t image_page_id = directory_page->GetBucketPageId(image_bucket_id);

  while (bucket_page->IsEmpty() && directory_page->GetGlobalDepth() != 0 &&
         directory_page->GetLocalDepth(image_bucket_id) == directory_page->GetLocalDepth(bucket_id)) {
    // 与insert相同，需要循环设置目录页
    uint32_t now_depth = directory_page->GetLocalDepth(bucket_id) - 1;
    uint32_t skip_entry_num = 1 << now_depth;
    int loop_count = 0;
    while (bucket_id + skip_entry_num * loop_count < directory_page->Size() &&
           image_bucket_id + skip_entry_num * loop_count < directory_page->Size()) {
      directory_page->SetBucketPageId(bucket_id + loop_count * skip_entry_num, image_page_id);
      directory_page->DecrLocalDepth(bucket_id + loop_count * skip_entry_num);
      directory_page->DecrLocalDepth(image_bucket_id + loop_count * skip_entry_num);
      loop_count++;
    }
    if (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
    bucket_page_guard.Drop();
    bpm_->DeletePage(bucket_page_id);
    bucket_page_guard = bpm_->FetchPageWrite(image_page_id);
    bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page_id = image_page_id;
    if (bucket_id > image_bucket_id) {
      bucket_id = image_bucket_id;
    }
    // 获取下一轮的image各种ID
    image_bucket_id = directory_page->GetSplitImageIndex(bucket_id, directory_page->GetLocalDepth(bucket_id));
    image_page_id = directory_page->GetBucketPageId(image_bucket_id);
    if (bucket_page_id == image_page_id) {
      break;
    }
    auto image_page_guard = bpm_->FetchPageWrite(image_page_id);
    auto image_page = image_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (!bucket_page->IsEmpty() && image_page->IsEmpty()) {
      // 特殊情况，bucketID不为空，并且imageId为空，需要交换一下。否则无法进入循环
      std::swap(bucket_id, image_bucket_id);
      std::swap(bucket_page_id, image_page_id);
      bucket_page_guard.Drop();
      bucket_page_guard = std::move(image_page_guard);
      bucket_page = image_page;
    }
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
