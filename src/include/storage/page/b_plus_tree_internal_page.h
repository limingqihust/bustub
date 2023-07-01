//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;
  void Init(int max_size = INTERNAL_PAGE_SIZE, page_id_t page_id = INVALID_PAGE_ID,
            page_id_t parent_page_id = INVALID_PAGE_ID);
  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueIndex(const ValueType &value) const -> int;
  auto ValueAt(int index) const -> ValueType;
  auto Lookup(const KeyType &key, const KeyComparator &comparator) const -> page_id_t;
  void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm);
  void CopyNFrom(MappingType *items, int size, BufferPoolManager *bpm);
  void InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);
  void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);

  void MoveAllTo(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm);
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm);
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm);
  void CopyLastFrom(const MappingType &item, BufferPoolManager *bpm);
  void CopyFirstFrom(const MappingType &item, BufferPoolManager *bpm);
  void RemoveAndDeleteRecord(int index);

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // first key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
