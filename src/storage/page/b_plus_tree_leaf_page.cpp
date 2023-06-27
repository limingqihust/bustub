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
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size,page_id_t page_id,page_id_t parent_page_id)
{
  SetMaxSize(max_size);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_page_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetPageType(IndexPageType::LEAF_PAGE);
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t
{
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id)
{
  next_page_id_=next_page_id;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  return array_[index].second;
}

/*
 * 根据key返回index
 * 如果命中直接返回index
 * 如果不命中返回应该插入的位置
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key,const KeyComparator &comparator) const -> int
{
  int index;
  for(index=0;index<GetSize();index++)
  {
    if(comparator(key,KeyAt(index))<=0)
    {
      break;
    }
  }
  return index;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> MappingType&
{
  return array_[index];
}

/*
 * 插入key-value
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType& key,const ValueType& value,const KeyComparator &comparator) ->int
{
  int index=KeyIndex(key,comparator);
  for(int i=GetSize();i>=index+1;i--)
  {
    array_[i]=array_[i-1];
  }
  array_[index]={key,value};
  IncreaseSize(1);
  return GetSize();
}

/*
 * 将本节点中的一半元素(向上取整)移动到另一个recipient节点中
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage* recipient, BufferPoolManager *bpm)
{
  int old_size=GetSize();
  int new_size=old_size/2;
  recipient->CopyNFrom(array_+new_size,old_size-new_size,bpm);
  SetSize(new_size);
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
}

/*
 * 从items处拷贝size个key-value对到本节点
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType* items, int size, BufferPoolManager *bpm)
{
  int old_size=GetSize();
  for(int i=old_size;i<old_size+size;i++)
  {
    array_[i]=*items;
    items++;
    IncreaseSize(1);
  }
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
