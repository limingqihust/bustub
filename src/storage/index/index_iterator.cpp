/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* leaf,int index,BufferPoolManager* bpm)
{
  leaf_=leaf;
  index_=index;
  bpm_=bpm;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator()
{
  if(leaf_!=nullptr)
  {
    bpm_->UnpinPage(leaf_->GetPageId(),false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool
{
  return (leaf_==nullptr || index_>=leaf_->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType &
{
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE &
{
  index_++;
  if(IsEnd())
  {
    page_id_t next_page_id=leaf_->GetNextPageId();
    bpm_->UnpinPage(leaf_->GetPageId(),false);
    if(next_page_id==INVALID_PAGE_ID)
    {
      leaf_=nullptr;
      index_=0;
      return *this;
    }
    Page* page=bpm_->FetchPage(next_page_id);
    leaf_=reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
    assert(leaf_!=nullptr);
    index_=0;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool
{
  return index_==itr.index_ && leaf_==itr.leaf_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool
{
  return index_!=itr.index_ || leaf_!=itr.leaf_;
}


template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
