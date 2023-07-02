//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 5, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}

TEST(BPlusTreeTests, DeleteTest2) {
  // create KeyComparator and index schema

  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 5, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::string output_filename = "output.dot";
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  tree.Draw(bpm, output_filename);
  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, output_filename);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}

TEST(BPlusTreeTests, MyTest1) {
  const int INTERNAL_NODE_SIZE = 4;  // Maximum size of internal nodes
  const int LEAF_NODE_SIZE = 2;      // Maximum size of leaf nodes
  std::string output_filename = "output.dot";
  // Create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // Create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);

  // Create B+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator,
                                                           LEAF_NODE_SIZE, INTERNAL_NODE_SIZE);

  // Create transaction
  auto *transaction = new Transaction(0);

  // Insert key-value pairs into the tree
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    RID rid(static_cast<int32_t>(key >> 32), value);
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  // Verify the correctness of inserted pairs
  for (auto key : keys) {
    std::vector<RID> rids;
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    bool is_present = tree.GetValue(index_key, &rids);
    EXPECT_TRUE(is_present);
    EXPECT_EQ(rids.size(), 1);
    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  tree.Draw(bpm, output_filename);

  // Remove specific keys from the tree
  std::vector<int64_t> remove_keys = {1, 5, 3, 7, 10, 14};
  for (auto key : remove_keys) {
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, output_filename);
  }

  // Verify the correctness of remaining pairs
  int64_t size = 0;
  for (auto key : keys) {
    std::vector<RID> rids;
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    bool is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      // Verify that the key has been removed from the tree
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      // Verify the correctness of remaining pairs
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size += 1;
    }
  }

  // Verify the final size of the tree
  EXPECT_EQ(size, keys.size() - remove_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}

TEST(BPlusTreeTests, MyTest2) {
  std::string output_filename = "output.dot";
  // Create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // Create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);

  // Create B+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 5, 3);

  // Create transaction
  auto *transaction = new Transaction(0);

  // Insert key-value pairs into the tree
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    RID rid(static_cast<int32_t>(key >> 32), value);
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    tree.Draw(bpm, output_filename);
  }

  // Verify the correctness of inserted pairs
  for (auto key : keys) {
    std::vector<RID> rids;
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    bool is_present = tree.GetValue(index_key, &rids);
    EXPECT_TRUE(is_present);
    EXPECT_EQ(rids.size(), 1);
    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  tree.Draw(bpm, output_filename);

  // Remove specific keys from the tree
  std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
  for (auto key : remove_keys) {
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, output_filename);
  }

  // Verify the correctness of remaining pairs
  int64_t size = 0;
  for (auto key : keys) {
    std::vector<RID> rids;
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    bool is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      // Verify that the key has been removed from the tree
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      // Verify the correctness of remaining pairs
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size += 1;
    }
  }

  // Verify the final size of the tree
  EXPECT_EQ(size, keys.size() - remove_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}

TEST(BPlusTreeTests, MyTest3) {
  std::string output_filename = "output.dot";
  // Create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // Create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);

  // Create B+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 5, 3);

  // Create transaction
  auto *transaction = new Transaction(0);

  // Add perserved_keys
  std::vector<int64_t> perserved_keys;
  std::vector<int64_t> dynamic_keys;
  int64_t total_keys = 50;
  int64_t sieve = 5;
  for (int64_t i = 1; i <= total_keys; i++) {
    if (i % sieve == 0) {
      perserved_keys.push_back(i);
    } else {
      dynamic_keys.push_back(i);
    }
  }

  for (auto key : perserved_keys) {
    int64_t value = key & 0xFFFFFFFF;
    RID rid(static_cast<int32_t>(key >> 32), value);
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    tree.Draw(bpm, output_filename);
  }

  for (auto key : dynamic_keys) {
    int64_t value = key & 0xFFFFFFFF;
    RID rid(static_cast<int32_t>(key >> 32), value);
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    tree.Draw(bpm, output_filename);
  }

  for (int key = 1; key <= 50; key++) {
    std::vector<RID> rids;
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    // Verify the correctness of remaining pairs
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    EXPECT_EQ(rids[0].GetSlotNum(), key);
  }

  for (auto key : dynamic_keys) {
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, output_filename);
  }

  // Verify the correctness of remaining pairs
  int64_t size = 0;
  for (int key = 1; key <= 50; key++) {
    std::vector<RID> rids;
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    bool is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      // Verify that the key has been removed from the tree
      EXPECT_NE(std::find(dynamic_keys.begin(), dynamic_keys.end(), key), dynamic_keys.end());
    } else {
      // Verify the correctness of remaining pairs
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size += 1;
    }
  }

  // Verify the final size of the tree
  EXPECT_EQ(size, perserved_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}

}  // namespace bustub
