#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto pNode = root_;
  bool bFound = true;
  for (auto c : key) {
    if (pNode and pNode->children_.find(c) != pNode->children_.end()) {
      pNode = pNode->children_.at(c);
    } else {
      bFound = false;
      break;
    }
  }
  if (bFound and pNode and pNode->is_value_node_) {
    // ValueGuard will hold a ref of root, which avoids dangling pointer
    auto pTrieNodeWithValue = dynamic_cast<const TrieNodeWithValue<T> *>(pNode.get());
    if (nullptr == pTrieNodeWithValue) {
      return nullptr;
    }
    return pTrieNodeWithValue->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  int size = key.size();
  // create value node first
  std::shared_ptr<TrieNode> new_mutable_node;
  if (nullptr == root_) {
   new_mutable_node = std::make_shared<TrieNode>();
   LOG_DEBUG("new_mutable_node is_value_node_ %d", new_mutable_node->is_value_node_);
  } else {
    new_mutable_node = std::shared_ptr<TrieNode>(root_->Clone());
  }

  std::shared_ptr<TrieNode> last_node;
  std::shared_ptr<const TrieNode> new_root = new_mutable_node;
  if (0 == key.size()) {
    new_root = std::make_shared<TrieNodeWithValue<T>>(new_mutable_node->children_, std::make_shared<T>(std::move(value)));
    return Trie(new_root);
  }
  for (int i = 0; i < size; i++) {
    auto c = key[i];
    if (new_mutable_node->children_.find(c) != new_mutable_node->children_.end()) {
      last_node = std::shared_ptr<TrieNode>(new_mutable_node->children_[c]->Clone());
    } else {
      last_node = std::make_shared<TrieNode>();
    }
    if (i == size -1) {
      // we should create a value node
      new_mutable_node->children_[c] = std::make_shared<TrieNodeWithValue<T>>(last_node->children_, std::make_shared<T>(std::move(value)));
      break;
    } else {
      new_mutable_node->children_[c] = last_node;
      new_mutable_node = last_node;
    }
  }
  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  // if this path doesn't contain any value, we should delete the entire path!
  if (nullptr == root_) {
    return Trie(root_);
  }
  int size = key.size();
  std::shared_ptr<TrieNode> new_mutable_node(root_->Clone());
  std::shared_ptr<TrieNode> last_node;
  std::shared_ptr<const TrieNode> new_root = new_mutable_node;
  if (0 == key.size()) {
    if (new_mutable_node->is_value_node_) {
       new_mutable_node->is_value_node_ = false;
    }
    return Trie(new_root);
  }
  // get last value node

  for (int i = 0; i < size; i++) {
    auto c = key[i];
    if (new_mutable_node->children_.find(c) != new_mutable_node->children_.end()) {
      last_node = std::shared_ptr<TrieNode>(new_mutable_node->children_[c]->Clone());
    } else {
      // not found let's quit
      break;
    }
    if (i == size - 1) {
      // last node ; delete or change to TrieNode
      if (last_node->children_.empty()) {
        new_mutable_node->children_.erase(c);
      } else if (last_node->is_value_node_) {
        new_mutable_node->children_[c] = std::make_shared<TrieNode>(last_node->children_);
        LOG_DEBUG("new_mutable_node after remove is_value_node:%d index:%d char:%c", new_mutable_node->children_[c]->is_value_node_, i, c);
      }
      break;
    } else {
      new_mutable_node->children_[c] = last_node;
      new_mutable_node = last_node;
    }
  }

  return Trie(new_root);
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
