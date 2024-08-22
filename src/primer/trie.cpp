#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  // Empty tree
  if (!root_) {
    return nullptr;
  }

  // find the objective node
  auto ptr = root_;
  for (auto c : key) {
    if (ptr->children_.count(c) != 0) {
      ptr = ptr->children_.at(c);
    } else {
      return nullptr;
    }
  }

  // check node type
  if (!ptr->is_value_node_) {
    return nullptr;
  }
  // check if match
  auto type_ptr = ptr.get();
  // return nullptr if type is error
  auto obj_node = dynamic_cast<const TrieNodeWithValue<T> *>(type_ptr);
  if (obj_node) {
    return obj_node->value_.get();
  }
  return nullptr;

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.")
  // check root
  if (!root_) {
    std::shared_ptr<TrieNode> root = std::make_shared<TrieNode>();
    return Trie(root).Put(key, std::move(value));
  }

  std::shared_ptr<TrieNode> root = root_->Clone();
  auto ptr = root;

  if (key.empty()) {
    std::shared_ptr<TrieNodeWithValue<T>> new_root =
        std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move(value)));
    return Trie(new_root);
  }

  for (size_t i{}; i < key.size() - 1; i++) {
    if (ptr->children_.count(key[i]) == 0) {
      // internal node exist
      std::shared_ptr<TrieNode> create_internal_node = std::make_shared<TrieNode>();
      // update children of ptr
      ptr->children_[key[i]] = create_internal_node;
      // update pointer
      ptr = create_internal_node;
    } else {
      // Clone the exist node
      std::shared_ptr<TrieNode> clone_node = ptr->children_[key[i]]->Clone();
      ptr->children_[key[i]] = clone_node;
      ptr = clone_node;
    }
  }
  // when function reach here, we can find the last needed node
  if (ptr->children_.count(key.back()) == 0) {
    std::shared_ptr<TrieNodeWithValue<T>> obj_node =
        std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    ptr->children_[key.back()] = obj_node;
  } else {
    std::shared_ptr<TrieNodeWithValue<T>> obj_node = std::make_shared<TrieNodeWithValue<T>>(
        ptr->children_[key.back()]->children_, std::make_shared<T>(std::move(value)));
    ptr->children_[key.back()] = obj_node;
  }
  return Trie(root);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

std::shared_ptr<const TrieNode> Trie::Dfs(const std::shared_ptr<const TrieNode> &root, std::string_view key,
                                          size_t index) const {
  // if find the last node, return the dealed node and stop recursion
  if (index == key.size()) {
    // leaf node
    if (root->children_.empty()) {
      return nullptr;
    }
    // internal node, return a new node and earse the content
    return std::make_shared<const TrieNode>(root->children_);
  }

  std::shared_ptr<const TrieNode> new_node;
  // delete const
  auto t = std::const_pointer_cast<TrieNode>(root);
  // if branch exists, deal with its childs recursively
  if (t->children_.find(key[index]) != t->children_.end()) {
    // return the updated child node
    new_node = Dfs(t->children_[key[index]], key, index + 1);
    // copy the node, finally a new tree
    auto node = root->Clone();
    // if child node is not nullptr
    if (new_node) {
      node->children_[key[index]] = new_node;
    } else {
      // child node is nullptr
      node->children_.erase(key[index]);
      // if the node doesn't have child, delete it
      if (!node->is_value_node_ && node->children_.empty()) {
        return nullptr;
      }
    }
    return node;
  }
  // if the branch doesn't exist, return node
  return root;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  auto root = Dfs(root_, key, 0);
  return Trie(root);
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
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
