//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size, page_id_t page_id, page_id_t parent_page_id) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_page_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetPageType(IndexPageType::LEAF_PAGE);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

/*
 * 根据key返回index
 * 如果命中直接返回index
 * 如果不命中返回应该插入的位置
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int index;
  for (index = 0; index < GetSize(); index++) {
    if (comparator(key, KeyAt(index)) <= 0) {
      break;
    }
  }
  return index;
}

/*
 * 有value获得index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  int index;
  for (index = 0; index < GetSize(); index++) {
    if (value == ValueAt(index)) {
      return index;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> MappingType & { return array_[index]; }

/*
 * 插入key-value
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  int index = KeyIndex(key, comparator);
  if (comparator(key, KeyAt(index)) == 0 && index < GetSize()) {
    return GetSize();
  }
  for (int i = GetSize(); i >= index + 1; i--) {
    array_[i] = array_[i - 1];
  }
  array_[index] = {key, value};
  IncreaseSize(1);
  return GetSize();
}

/*
 * 将本节点中的一半元素(向上取整)移动到另一个recipient节点中
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *bpm) {
  int old_size = GetSize();
  int new_size = old_size / 2;
  recipient->CopyNFrom(array_ + new_size, old_size - new_size, bpm);
  SetSize(new_size);
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
}

/*
 * 从items处拷贝size个key-value对到本节点
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *bpm) {
  int old_size = GetSize();
  for (int i = old_size; i < old_size + size; i++) {
    array_[i] = *items;
    items++;
    IncreaseSize(1);
  }
}

/*
 * 在该叶子节点中删除key对应的键值对
 * 删除后size可能小于最小大小 会在后续处理
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> int {
  int index = KeyIndex(key, comparator);  // 找到要删除的key-value对的index
  assert(comparator(KeyAt(index), key) == 0);
  int old_size = GetSize();
  for (int i = index; i < old_size - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return GetSize();
}

/*
 * recipient ----> this
 * 将this节点的全部节点移动到recipient节点后面
 * 本结点和recipient节点均以在buffer pool中
 * this节点需要删除 在后续中完成
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, BufferPoolManager *bpm) {
  int this_old_size = GetSize();
  int recipient_old_size = recipient->GetSize();
  for (int i = 0; i < this_old_size; i++) {
    recipient->array_[recipient_old_size + i] = array_[i];
  }
  SetSize(0);
  recipient->IncreaseSize(this_old_size);
  recipient->SetNextPageId(GetNextPageId());
}

/*
 * recipient----> this
 * 将this节点的第一个元素移动到recipient的尾部
 * this节点和recipient均已在buffer pool中
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, BufferPoolManager *bpm) {
  int this_old_size = GetSize();
  MappingType &item = array_[0];
  recipient->CopyLastFrom(item, bpm);
  for (int i = 0; i < this_old_size - 1; i++) {
    array_[i + 1] = array_[i];
  }
  IncreaseSize(-1);
}

/*
 * this ----> recipient
 * 把this节点的最后一个移动到recipient节点的头部
 * this节点和recipient节点均已在buffer pool中
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, BufferPoolManager *bpm) {
  MappingType &item = array_[GetSize() - 1];
  recipient->CopyFirstFrom(item, bpm);
  IncreaseSize(-1);
}

/*
 * 从item处复制key-value对到本结点的最后
 * 本节点已在buffer pool中
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item, BufferPoolManager *bpm) {
  int old_size = GetSize();
  array_[old_size] = item;
  IncreaseSize(1);
}

/*
 * 从item处复制key-value对到本结点的开始
 * 本节点已在buffer pool中
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item, BufferPoolManager *bpm) {
  int old_size = GetSize();
  for (int i = old_size; i >= 1; i--) {
    array_[i] = array_[i - 1];
  }
  array_[0] = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
