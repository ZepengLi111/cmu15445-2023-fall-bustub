//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_test.cpp
//
// Identification: test/container/disk/hash/extendible_htable_test.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"
#include "storage/disk/disk_manager_memory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 0, 2, 2);

  int num_keys = 8;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // attempt another insert, this should fail because table is full
  ASSERT_FALSE(ht.Insert(num_keys, num_keys));
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest2) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, RemoveTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  // remove the keys we inserted
  for (int i = 0; i < num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_TRUE(removed);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  // try to remove some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_FALSE(removed);
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

TEST(ExtendibleHTableTest, RemoveTest2) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  int key[5] = {-2147483648, -2147483647, -2147483646, -2147483645, -2147483644};

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(key[i], key[i]);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(key[i], &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(key[i], res[0]);
  }

  ht.PrintHT();

  ht.VerifyIntegrity();

  // try to remove some keys that don't exist/were not inserted
  for (int i = 0; i < num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_FALSE(removed);
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  // remove the keys we inserted
  for (int i = 0; i < num_keys; i++) {
    bool removed = ht.Remove(key[i]);
    ASSERT_TRUE(removed);
    std::vector<int> res;
    ht.GetValue(key[i], &res);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

TEST(ExtendibleHTableTest, RemoveTest3) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 1, 2, 2);

  int num_keys = 4;
  int key[4] = {4, 5, 6, 14};

  int num_keys2 = 3;
  int key2[3] = {5, 14, 4};

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(key[i], 0);
    ASSERT_TRUE(inserted);
    //    std::vector<int> res;
    //    ht.GetValue(key[i], &res);
    //    ASSERT_EQ(1, res.size());
    //    ASSERT_EQ(key[i], res[0]);
  }

  ht.VerifyIntegrity();

  // remove the keys we inserted
  for (int i = 0; i < num_keys2; i++) {
    bool removed = ht.Remove(key2[i]);
    ASSERT_TRUE(removed);
    //    std::vector<int> res;
    //    ht.GetValue(key2[i], &res);
    //    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

TEST(ExtendibleHTableTest, InsertTest3) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(3, disk_mgr.get(), 10);

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 9, 9,
                                                      10);
  // insert some values
  for (int i = 0; i < 11; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
  }
  std::cout << std::endl;
  ht.PrintHT();

  ht.VerifyIntegrity();

  //  // remove the keys we inserted
  //  for (int i = 0; i < 500; i++) {
  //    bool removed = ht.Remove(i);
  //    ASSERT_TRUE(removed);
  //  }
  //  std::cout << std::endl;
  //  ht.PrintHT();
  //
  //  ht.VerifyIntegrity();
  //
  //  for (int i = 1000; i < 1500; i++) {
  //    bool inserted = ht.Insert(i, i);
  //    ASSERT_TRUE(inserted);
  //  }
  //  std::cout << std::endl;
  //  ht.PrintHT();
  //
  //  ht.VerifyIntegrity();
  //
  //  // remove the keys we inserted
  //  for (int i = 500; i < 1000; i++) {
  //    bool removed = ht.Remove(i);
  //    ASSERT_TRUE(removed);
  //  }
  //  std::cout << std::endl;
  //  ht.PrintHT();
  //
  //  ht.VerifyIntegrity();
  //
  //  for (int i = 0; i < 500; i++) {
  //    bool inserted = ht.Insert(i, i);
  //    ASSERT_TRUE(inserted);
  //  }
  //  std::cout << std::endl;
  //  ht.PrintHT();
  //
  //  ht.VerifyIntegrity();
  //
  //  // remove the keys we inserted
  //  for (int i = 1000; i < 1500; i++) {
  //    bool removed = ht.Remove(i);
  //    ASSERT_TRUE(removed);
  //  }
  //  std::cout << std::endl;
  //  ht.PrintHT();
  //
  //  ht.VerifyIntegrity();
  //
  //  // remove the keys we inserted
  //  for (int i = 0; i < 500; i++) {
  //    bool removed = ht.Remove(i);
  //    ASSERT_TRUE(removed);
  //  }
  //  std::cout << std::endl;
  //  ht.PrintHT();
  //
  //  ht.VerifyIntegrity();
  //
  //  for (int i = 1000; i < 1500; i++) {
  //    bool inserted = ht.Insert(i, i);
  //    ASSERT_TRUE(inserted);
  //  }
  //  std::cout << std::endl;
  //  ht.PrintHT();
  //
  //  ht.VerifyIntegrity();
  //
  //  // remove the keys we inserted
  //  for (int i = 500; i < 1000; i++) {
  //    bool removed = ht.Remove(i);
  //    ASSERT_FALSE(removed);
  //  }
  //  std::cout << std::endl;
  //  ht.PrintHT();
  //
  //  ht.VerifyIntegrity();
  //
  //  for (int i = 0; i < 500; i++) {
  //    bool inserted = ht.Insert(i, i);
  //    ASSERT_TRUE(inserted);
  //  }
  //  std::cout << std::endl;
  //  ht.PrintHT();
  //
  //  ht.VerifyIntegrity();
  //
  //  // remove the keys we inserted
  //  for (int i = 1000; i < 1500; i++) {
  //    bool removed = ht.Remove(i);
  //    ASSERT_TRUE(removed);
  //  }
  //  std::cout << std::endl;
  //  ht.PrintHT();
  //
  //  ht.VerifyIntegrity();
  //
  //  // remove the keys we inserted
  //  for (int i = 0; i < 1500; i++) {
  //    ht.Remove(i);
  ////    ASSERT_TRUE(removed);
  //  }
  //  std::cout << std::endl;
  //  ht.PrintHT();
  //
  //  ht.VerifyIntegrity();
}

}  // namespace bustub
