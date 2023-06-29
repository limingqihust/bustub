#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (key.empty()) {
    // if key is empty string,should return value in root_
    if (!root_->is_value_node_) {
      return nullptr;
    }
    auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(root_.get());
    if (value_node == nullptr) {
      return nullptr;
    }
    return value_node->value_.get();
  }

  auto current_node = root_;
  for (char c : key) {
    if (!current_node->children_.count(c)) {
      // do not find value corresponding to key,should return nullptr
      return nullptr;
    }
    current_node = current_node->children_.at(c);
  }
  if (!current_node->is_value_node_) {
    return nullptr;
  }
  // find node corresponding to key
  auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(current_node.get());
  if (value_node == nullptr) {
    return nullptr;
  }
  return value_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  auto new_root = root_ ? root_->Clone() : std::make_shared<TrieNode>();
  if (key.empty()) {
    // if key is empty,insert value into new_root
    auto new_children = new_root->children_;
    auto new_value = std::make_shared<T>(std::move(value));  // ptr to value
    auto new_value_node = std::make_shared<TrieNodeWithValue<T>>(new_children, new_value);
    new_value_node->is_value_node_ = true;
    return Trie(std::move(std::static_pointer_cast<const TrieNode>(new_value_node)));
  }

  auto current_node = new_root.get();
  for (auto it = key.begin(); it < key.end() - 1; it++) {
    const char c = *it;
    if (current_node->children_.count(c)) {
      current_node->children_[c] = current_node->children_[c]->Clone();
    } else {
      current_node->children_[c] = std::make_shared<TrieNode>(std::map<char, std::shared_ptr<const TrieNode>>());
    }

    current_node = const_cast<TrieNode *>(current_node->children_.at(c).get());
  }

  if (current_node->children_.count(key.back())) {
    // have node with value key.back()
    auto new_children = current_node->children_[key.back()]->children_;
    auto new_value = std::make_shared<T>(std::move(value));
    auto new_value_node = std::make_shared<TrieNodeWithValue<T>>(new_children, new_value);
    new_value_node->is_value_node_ = true;
    current_node->children_[key.back()] = new_value_node;
  } else {
    // do not have node with value key.back()
    std::map<char, std::shared_ptr<const TrieNode>> new_children;
    auto new_value = std::make_shared<T>(std::move(value));
    auto new_value_node = std::make_shared<TrieNodeWithValue<T>>(new_children, new_value);
    new_value_node->is_value_node_ = true;
    current_node->children_[key.back()] = std::static_pointer_cast<const TrieNode>(new_value_node);
  }
  return Trie(std::move(new_root));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  auto new_root = root_ ? root_->Clone() : std::make_unique<TrieNode>();

  std::stack<std::pair<TrieNode *, char>> _stack;
  auto current_node = new_root.get();
  for (auto it = key.begin(); it < key.end(); it++) {
    const char c = *it;
    if (current_node->children_.count(c) != 0) {
      current_node->children_[c] = current_node->children_[c]->Clone();
    } else {
      // if do not have proper path,directly return old root
      // it may rarely happen
      return Trie(root_->Clone());
    }
    current_node = const_cast<TrieNode *>(current_node->children_[c].get());
    _stack.push({current_node, c});
  }

  // we should remove current[key.back()]
  // if current[key.back()] has children,transfer cufrrent[key.back()] to TrieNode
  // if it do not have children,delete it
  // roll back to see whether we has more node to delete
  if (!current_node->is_value_node_) {
    // if this node is not value_node,return old root
    return Trie(root_->Clone());
  }

  char c = _stack.top().second;
  current_node = _stack.top().first;
  _stack.pop();
  auto current_parent_node = _stack.top().first;
  if (!current_node->children_.empty()) {
    current_parent_node->children_[c] = std::make_unique<TrieNode>(current_node->children_);
    return Trie(std::move(new_root));
  } else {
    current_parent_node->children_.erase(c);
  }
  while (_stack.size() >= 2) {
    c = _stack.top().second;
    current_node = _stack.top().first;
    _stack.pop();
    current_parent_node = _stack.top().first;
    if (!current_node->children_.empty() || current_node->is_value_node_) {
      break;
      current_parent_node->children_.erase(c);
    }
  }

  return Trie(std::move(new_root));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
