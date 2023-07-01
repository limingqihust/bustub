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

/*
 * recipient ----> this
 * 将this结点的所有key-value对移动到recipient节点的尾部
 * this节点和recipient节点均已在buffer pool中
 * 需要更新this结点的子结点的父节点属性
 * 需要更新recipient节点的size
 * 需要删除this节点 在后续完成中完成
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm) {
  assert(recipient->GetSize() + GetSize() <= GetMaxSize());
  page_id_t recipient_page_id = recipient->GetPageId();
  int this_old_size = GetSize();
  int recipient_old_size = recipient->GetSize();
  for (int i = 0; i < this_old_size; i++) {  // 需要更新子结点的父节点属性
    recipient->array_[recipient_old_size + i] = array_[i];
    Page *child_page = bpm->FetchPage(array_[i].second);
    assert(child_page != nullptr);
    auto child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    assert(child_tree_page != nullptr);
    child_tree_page->SetParentPageId(recipient_page_id);
    bpm->UnpinPage(child_page->GetPageId(), true);
  }
  SetSize(0);
  recipient->IncreaseSize(this_old_size);
}

/*
 * recipient ----- this
 * 将this的第一个key-value对移动到recipient节点的尾部
 * this和recipient均已在buffer pool中
 * 用于重分配时从右节点借一个元素到左节点
 * 更改该节点的父节点在CopyLastFrom()中进行
 * 需要更改this节点的父节点 在Redistribute()中进行
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm) {
  int this_old_size = GetSize();
  MappingType &item = array_[0];
  recipient->CopyLastFrom(item, bpm);            // 将首节点移动到recipient节点中
  for (int i = 0; i < this_old_size - 1; i++) {  // 将后面的前移一位
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);  // 更新this节点的size
}

/*
 * this ----> recipient
 * 将this节点的最后一个key-value对移动到recipient节点的头部
 * 需要更改移动节点的父节点 在CopyFirstFrom中进行
 * 需要更改recipient的父节点 在Redistribute()函数中进行
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm) {
  MappingType &item = array_[GetSize() - 1];
  recipient->CopyFirstFrom(item, bpm);
  IncreaseSize(-1);
}

/*
 * 从item处复制一个节点到this节点的尾部
 * 更新本结点的size
 * 更新移动节点的父节点
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &item, BufferPoolManager *bpm) {
  int old_size = GetSize();
  array_[old_size] = item;
  IncreaseSize(1);
  Page *item_page = bpm->FetchPage(item.second);
  assert(item_page != nullptr);
  auto item_tree_page = reinterpret_cast<BPlusTreeInternalPage *>(item_page->GetData());
  assert(item_tree_page != nullptr);
  item_tree_page->SetParentPageId(GetPageId());
  bpm->UnpinPage(item_page->GetPageId(), true);
}

/*
 * 从item处复制一个节点到this节点的头部
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &item, BufferPoolManager *bpm) {
  int old_size = GetSize();
  page_id_t this_page_id = GetPageId();
  for (int i = old_size; i >= 1; i--) {
    array_[i] = array_[i - 1];
  }
  array_[0] = item;
  IncreaseSize(1);
  Page *item_page = bpm->FetchPage(item.second);
  assert(item_page != nullptr);
  auto item_tree_page = reinterpret_cast<BPlusTreeInternalPage *>(item_page->GetData());
  assert(item_tree_page != nullptr);
  item_tree_page->SetParentPageId(this_page_id);
  bpm->UnpinPage(item_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndDeleteRecord(int index) {
  int old_size = GetSize();
  for (int i = index; i < old_size - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
