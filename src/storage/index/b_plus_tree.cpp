#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
  //  LOG_INFO("# BPlusTree : leaf_max_size : %d internal_max_size : %d", internal_max_size, leaf_max_size);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto root_page_id = GetRootPageId();
  return root_page_id == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  latch_.RLock();
  //  LOG_INFO("# GetValue : key %ld", key.ToString());
  bool found_flag = false;
  Page *page = FindLeafPage(key);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  if (leaf_page == nullptr) {
    bpm_->UnpinPage(page->GetPageId(), false);
    //    LOG_INFO("# GetValue : key %ld done", key.ToString());
    latch_.RUnlock();
    return false;
  }
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      result->push_back(leaf_page->ValueAt(i));
      found_flag = true;
    }
  }
  bpm_->UnpinPage(page->GetPageId(), false);
  //  LOG_INFO("# GetValue : key %ld done", key.ToString());
  latch_.RUnlock();
  return found_flag;
  //  Context ctx;
  //  (void)ctx;
  //  return false;
}

/*
 * 根据给定的key找到目标的page
 * 返回时该page已被fetch到bpm中
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) const -> Page * {
  page_id_t page_id = GetRootPageId();
  assert(page_id != INVALID_PAGE_ID);
  while (true) {
    Page *page = bpm_->FetchPage(page_id);
    assert(page != nullptr);
    auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (tree_page->IsLeafPage()) {  // 判断这个page是不是叶子节点
      return page;
    }
    auto internal_page = reinterpret_cast<InternalPage *>(tree_page);

    page_id = internal_page->Lookup(key, comparator_);

    bpm_->UnpinPage(page->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftLeafPage() const -> Page * {
  page_id_t page_id = GetRootPageId();
  assert(page_id != INVALID_PAGE_ID);
  while (true) {
    Page *page = bpm_->FetchPage(page_id);
    assert(page != nullptr);
    auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (tree_page->IsLeafPage()) {
      return page;
    }
    auto internal_page = reinterpret_cast<InternalPage *>(tree_page);
    page_id = internal_page->ValueAt(0);
    bpm_->UnpinPage(page->GetPageId(), false);
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  latch_.WLock();
  //  LOG_INFO("# Insert :  key %ld value %s", key.ToString(), value.ToString().c_str());
  if (IsEmpty()) {
    StartNewTree(key, value);
    //    LOG_INFO("# Insert : key %ld done", key.ToString());
    latch_.WUnlock();
    return true;
  }
  auto result = InsertIntoLeaf(key, value);
  //  LOG_INFO("# Insert : key %ld done", key.ToString());
  latch_.WUnlock();
  return result;
  //  Context ctx;
  //  (void)ctx;
  //  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_root_page_id;
  auto new_root_page = bpm_->NewPage(&new_root_page_id);
  assert(new_root_page != nullptr);
  auto leaf_page = reinterpret_cast<LeafPage *>(new_root_page->GetData());
  leaf_page->Init(leaf_max_size_, new_root_page_id, INVALID_PAGE_ID);
  leaf_page->Insert(key, value, comparator_);
  bpm_->UnpinPage(new_root_page_id, true);
  SetRootPageId(new_root_page_id);
}

/*
 * 将key-value插入到一个叶子节点中
 * 1.找到正确的叶子节点
 * 2.调用BPlusTreeLeafPage的Insert方法将key-value插入到叶子节点中
 * 3.插入后判断叶子节点的size是否超过了max_size
 * 4.超过了需要调用Split()和InsertIntoParent()进行后续处理
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value) -> bool {
  auto page = FindLeafPage(key);
  assert(page != nullptr);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  auto old_size = leaf_page->GetSize();
  auto new_size = leaf_page->Insert(key, value, comparator_);
  if (old_size == new_size) {  // insert失败 存在重复的key
    bpm_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  if (new_size < leaf_max_size_ - 1) {  // 节点未满
    bpm_->UnpinPage(page->GetPageId(), true);
    return true;
  }
  // 节点已满 需要分裂
  auto new_leaf_page = Split(leaf_page);
  assert(new_leaf_page != nullptr);
  auto split_key = new_leaf_page->KeyAt(0);
  InsertIntoParent(leaf_page, split_key, new_leaf_page);
  bpm_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

/*
 * old_tree_page中的一半元素已被移动到new_tree_page,
 * 现将old_tree_page和new_tree_page放到它们的父节点中去
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_tree_page, const KeyType &key, BPlusTreePage *new_tree_page) {
  page_id_t parent_page_id = old_tree_page->GetParentPageId();
  if (parent_page_id != INVALID_PAGE_ID) {  // old_tree_page不是根节点
    Page *parent_page = bpm_->FetchPage(parent_page_id);
    auto parent_tree_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
    new_tree_page->SetParentPageId(parent_page_id);
    parent_tree_page->InsertNodeAfter(old_tree_page->GetPageId(), key, new_tree_page->GetPageId());
    if (parent_tree_page->GetSize() > parent_tree_page->GetMaxSize()) {
      auto new_parent_tree_page = Split(parent_tree_page);
      InsertIntoParent(parent_tree_page, new_parent_tree_page->KeyAt(0), new_parent_tree_page);
    }
    bpm_->UnpinPage(parent_tree_page->GetPageId(), true);
    bpm_->UnpinPage(new_tree_page->GetPageId(), true);
  } else {  // old_tree_page是根节点 没有父节点存在
    page_id_t new_root_page_id;
    Page *new_page = bpm_->NewPage(&new_root_page_id);
    assert(new_root_page_id != INVALID_PAGE_ID);
    assert(new_page != nullptr);
    auto new_root_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    assert(new_root_page != nullptr);
    new_root_page->Init(internal_max_size_, new_root_page_id, INVALID_PAGE_ID);
    new_root_page->PopulateNewRoot(old_tree_page->GetPageId(), key, new_tree_page->GetPageId());
    old_tree_page->SetParentPageId(new_root_page_id);
    new_tree_page->SetParentPageId(new_root_page_id);
    SetRootPageId(new_root_page_id);
    bpm_->UnpinPage(new_root_page_id, true);
    bpm_->UnpinPage(new_tree_page->GetPageId(), true);
  }
}

/*
 * node为要分裂的节点 返回新生成的节点 和参数节点是同一类型
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *tree_page) -> N * {
  page_id_t new_page_id;
  Page *new_page = bpm_->NewPage(&new_page_id);
  assert(new_page_id != INVALID_PAGE_ID);
  assert(new_page != nullptr);
  N *new_tree_page = reinterpret_cast<N *>(new_page->GetData());
  if (tree_page->IsLeafPage()) {
    new_tree_page->Init(leaf_max_size_, new_page_id, tree_page->GetParentPageId());
  } else {
    new_tree_page->Init(internal_max_size_, new_page_id, tree_page->GetParentPageId());
  }
  tree_page->MoveHalfTo(new_tree_page, bpm_);
  return new_tree_page;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * 如果为空直接返回
 * 如果非空 找到正确的叶子节点 并删除对应的entry
 * 如果删除后小于最小大小 调用CoalesceOrRedistribute()进行重分配或者合并
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  latch_.WLock();
  //  LOG_INFO("# Remove : key %ld", key.ToString());
  if (IsEmpty()) {  // 树为空 直接返回
                    //    LOG_INFO("# Remove : key %ld done ", key.ToString());
    latch_.WUnlock();
    return;
  }
  Page *page = FindLeafPage(key);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  assert(leaf_page != nullptr);
  //  LOG_INFO("# Remove : item to be removed is in page %d", leaf_page->GetPageId());
  int remove_index = leaf_page->KeyIndex(key, comparator_);
  if (comparator_(leaf_page->KeyAt(remove_index), key) != 0 || remove_index >= leaf_page->GetSize()) {  // 该key不存在
    bpm_->UnpinPage(page->GetPageId(), false);
    //    LOG_INFO("# Remove : key %ld done", key.ToString());
    latch_.WUnlock();
    return;
  }
  int leaf_page_new_size = leaf_page->RemoveAndDeleteRecord(key, comparator_);

  if (leaf_page_new_size >= leaf_page->GetMinSize()) {  // 删除后size大于最小大小
    bpm_->UnpinPage(page->GetPageId(), true);
    //    LOG_INFO("# Remove : key %ld done", key.ToString());
    latch_.WUnlock();
    return;
  }
  //  LOG_INFO("# Remove : after remove,leaf_page_new_size is %d,need to coalesce or redistribute", leaf_page_new_size);
  CoalesceOrRedistribute(leaf_page);  // 删除后该叶子节点的size小于最大大小 需要重分配或者合并
  bpm_->UnpinPage(page->GetPageId(), true);
  //  LOG_INFO("# Remove : key %ld done", key.ToString());
  latch_.WUnlock();
  //  Context ctx;
  //  (void)ctx;
}

/*
 * 节点N的大小小于最小大小 需要从兄弟节点借一个节点后者和兄弟合并
 * 当node为根节点时 调用AdjustRoot()修改根节点
 * 首先调用FindSibling()找到兄弟节点
 * 如果可以从兄弟节点那里借一个 调用Redistribute()借一个节点
 * 如果兄弟节点无法借一个 调用Coalesce()将两个节点进行合并
 * node已被取到buffer pool中 会在后续操作中unpin
 * 在FindSibling将兄弟节点取到buffer pool中 在本函数中将其unpin
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node) {
  if (node->IsRootPage()) {
    //    LOG_INFO("# CoalesceOrRedistribute : need to AdjustRoot");
    if (node->GetSize() == 1 && !node->IsLeafPage()) {
      AdjustRoot(node);
    }
    return;
  }
  N *sibling_node;
  bool is_right_brother = FindSibling(node, sibling_node);  // 兄弟节点已被取到buffer pool中
  //  LOG_INFO("# CoalesceOrRedistribute : sibling_node chosen is page %d", sibling_node->GetPageId());
  if (sibling_node->GetSize() >= sibling_node->GetMinSize() + 1) {  // 从兄弟节点借一个节点 并更新父节点
    if (is_right_brother) {  // node ----> sibling node 将sibling中的节点移动到this节点中
      Redistribute(node, sibling_node, is_right_brother);
    } else {
      Redistribute(sibling_node, node, is_right_brother);
    }
    assert(node->GetSize() == node->GetMinSize());
    assert(sibling_node->GetSize() >= sibling_node->GetMinSize());
    bpm_->UnpinPage(sibling_node->GetPageId(), true);
  } else {  // 和兄弟节点合并
    if (is_right_brother) {
      Coalesce(node, sibling_node);
    } else {
      Coalesce(sibling_node, node);
    }
    bpm_->UnpinPage(sibling_node->GetPageId(), true);
  }
}

/*
 * 寻找node节点的兄弟节点
 * 若兄弟节点为右节点返回true 为左节点返回false
 * node已被取到buffer pool中
 * 函数返回时 sibling已经被取到buffer pool中
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::FindSibling(N *node, N *&sibling) -> bool {
  page_id_t me_page_id = node->GetPageId();
  page_id_t parent_page_id = node->GetParentPageId();
  Page *parent_page = bpm_->FetchPage(parent_page_id);
  assert(parent_page != nullptr);
  auto parent_tree_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  assert(parent_tree_page != nullptr);
  int me_index = parent_tree_page->ValueIndex(me_page_id);
  assert(me_index != -1);
  // 优先选择左边的兄弟节点
  int sibling_index = (me_index == 0) ? me_index + 1 : me_index - 1;
  page_id_t sibling_page_id = parent_tree_page->ValueAt(sibling_index);
  Page *sibling_page = bpm_->FetchPage(sibling_page_id);
  assert(sibling_page != nullptr);
  sibling = reinterpret_cast<N *>(sibling_page->GetData());
  assert(sibling != nullptr);

  bpm_->UnpinPage(parent_page_id, false);
  return me_index == 0;
}

/*
 * 将right_node节点合并到left_node节点中
 * 并更新他们的父节点
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(N *left_node, N *right_node) {
  //  LOG_INFO("# Coalesce : coalesce page %d to page %d", right_node->GetPageId(), left_node->GetPageId());
  page_id_t parent_page_id = right_node->GetParentPageId();
  Page *parent_page = bpm_->FetchPage(parent_page_id);
  assert(parent_page != nullptr);
  auto parent_tree_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  assert(parent_tree_page != nullptr);
  int remove_index = parent_tree_page->ValueIndex(right_node->GetPageId());
  assert(remove_index != -1);
  right_node->MoveAllTo(left_node, bpm_);
  parent_tree_page->RemoveAndDeleteRecord(remove_index);
  if (parent_tree_page->GetSize() < parent_tree_page->GetMinSize()) {
    CoalesceOrRedistribute(parent_tree_page);
  }

  bpm_->UnpinPage(parent_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *left_node, N *right_node, bool is_right_brother) {
  if (is_right_brother) {  // right_node移动一个与元素到left_node             left_node <---- right_node
    //    LOG_INFO("# Redistribute : page %d lend first item to page %d", right_node->GetPageId(),
    //    left_node->GetPageId());
    assert(left_node->GetSize() == left_node->GetMinSize() - 1);
    assert(right_node->GetSize() >= right_node->GetMinSize() + 1);
    right_node->MoveFirstToEndOf(left_node, bpm_);
    assert(left_node->GetSize() == left_node->GetMinSize());
    assert(right_node->GetSize() >= right_node->GetMinSize());

    // 更改父节点
    page_id_t parent_page_id = left_node->GetParentPageId();
    Page *parent_page = bpm_->FetchPage(parent_page_id);
    assert(parent_page != nullptr);
    auto parent_tree_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
    assert(parent_tree_page != nullptr);
    auto alter_index = parent_tree_page->ValueIndex(right_node->GetPageId());
    assert(alter_index != -1);
    parent_tree_page->SetKeyAt(alter_index, right_node->KeyAt(0));
    bpm_->UnpinPage(parent_page_id, true);
  } else {  // left_node移动一个元素到right_node               left_node---->right_node
            //    LOG_INFO("# Redistribute : page %d lend last item to page %d", left_node->GetPageId(),
            //    right_node->GetPageId());
    assert(left_node->GetSize() >= left_node->GetMinSize() + 1);
    assert(right_node->GetSize() == right_node->GetMinSize() - 1);
    left_node->MoveLastToFrontOf(right_node, bpm_);
    assert(left_node->GetSize() >= left_node->GetMinSize());
    assert(right_node->GetSize() == right_node->GetMinSize());

    // 更改父节点
    page_id_t parent_page_id = left_node->GetParentPageId();
    Page *parent_page = bpm_->FetchPage(parent_page_id);
    assert(parent_page != nullptr);
    auto parent_tree_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
    assert(parent_tree_page != nullptr);
    auto alter_index = parent_tree_page->ValueIndex(right_node->GetPageId());
    assert(alter_index != -1);
    parent_tree_page->SetKeyAt(alter_index, right_node->KeyAt(0));
    bpm_->UnpinPage(parent_page_id, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_page) {
  assert(old_root_page->GetSize() == 1);
  auto old_root_tree_page = reinterpret_cast<InternalPage *>(old_root_page);
  page_id_t new_root_page_id = old_root_tree_page->ValueAt(0);
  Page *new_root_page = bpm_->FetchPage(new_root_page_id);
  auto new_root_tree_page = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
  assert(new_root_tree_page != nullptr);
  new_root_tree_page->SetParentPageId(INVALID_PAGE_ID);
  SetRootPageId(new_root_page_id);
  bpm_->UnpinPage(new_root_page_id, true);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  //  LOG_INFO("# Begin : first");
  if (IsEmpty()) {
    return End();
  }
  Page *first_page = FindLeftLeafPage();
  assert(first_page != nullptr);
  auto first_tree_page = reinterpret_cast<LeafPage *>(first_page->GetData());
  return INDEXITERATOR_TYPE(first_tree_page, 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  //  LOG_INFO("# Begin : key %ld", key.ToString());
  Page *page = FindLeafPage(key);
  assert(page != nullptr);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int index = leaf_page->KeyIndex(key, comparator_);
  assert(comparator_(key, leaf_page->KeyAt(index)) == 0);
  return INDEXITERATOR_TYPE(leaf_page, index, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, 0, bpm_); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  Page *page = bpm_->FetchPage(header_page_id_);
  auto header_page = reinterpret_cast<BPlusTreeHeaderPage *>(page->GetData());
  page_id_t root_page_id = header_page->root_page_id_;
  bpm_->UnpinPage(header_page_id_, false);
  return root_page_id;
  //  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  //  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  //  return root_page->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(page_id_t root_page_id) {
  Page *page = bpm_->FetchPage(header_page_id_);
  auto header_page = reinterpret_cast<BPlusTreeHeaderPage *>(page->GetData());
  header_page->root_page_id_ = root_page_id;
  bpm_->UnpinPage(header_page_id_, false);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
