//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size, page_id_t page_id, page_id_t parent_page_id) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_page_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index != 0);
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (value == ValueAt(i)) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> page_id_t {
  auto page_id = ValueAt(GetSize() - 1);
  for (int i = 1; i < GetSize(); i++) {
    if (comparator(KeyAt(i), key) > 0) {  // 寻找第一个比key大的键
      page_id = ValueAt(i - 1);
      break;
    }
  }
  return page_id;
}

/*
 * 从本结点拷贝一般的key-value到recipient结点
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm) {
  int old_size = GetSize();
  int new_size = old_size / 2;
  recipient->CopyNFrom(array_ + new_size, old_size - new_size, bpm);
  SetSize(new_size);
}

/*
 * 从items处赋值size个key-value对本节点的尾部
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *bpm) {
  int old_size = GetSize();
  for (int i = old_size; i < old_size + size; i++) {
    array_[i] = *items;
    items++;
    Page *page = bpm->FetchPage(array_[i].second);
    auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    tree_page->SetParentPageId(GetPageId());
    bpm->UnpinPage(array_[i].second, true);
    IncreaseSize(1);
  }
}

/*
 * 在old_value元素之后插入一个新的key-value对
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  int old_index = ValueIndex(old_value);
  assert(old_index != -1);
  for (int i = GetSize(); i >= old_index + 2; i--) {
    array_[i] = array_[i - 1];
  }
  array_[old_index + 1] = {new_key, new_value};
  IncreaseSize(1);
}

/*
 * 产生一个新的根节点
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1] = {new_key, new_value};
  SetSize(2);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
