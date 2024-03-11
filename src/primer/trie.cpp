#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  //  throw NotImplementedException("Trie::Get is not implemented.");
  if (this->GetRoot() == nullptr) {
    return nullptr;
  }
  auto node = this->GetRoot();
  for (auto i : key) {
    auto value = node->children_.find(i);
    if (value != node->children_.end()) {
      node = value->second;
    } else {
      // case 1: can not find the key
      return nullptr;
    }
  }
  // case 2-1: the node do not have value
  if (!node->is_value_node_) {
    return nullptr;
  }
  auto node_ptr = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  // case 2-2: the node's value is mismatched
  if (node_ptr == nullptr) {
    return nullptr;
  }
  // case 3: return value
  return node_ptr->value_.get();
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //  throw NotImplementedException("Trie::Put is not implemented.");

  std::shared_ptr<TrieNode> root = nullptr;
  if (this->GetRoot() == nullptr) {
    // resolve the case that root is nullptr
    root = std::make_shared<TrieNode>();
  } else {
    auto trie_root = this->GetRoot();
    auto new_root = trie_root->Clone();
    root = std::shared_ptr<TrieNode>(std::move(new_root));
  }
  auto node = root;
  auto value_ptr = std::make_shared<T>(std::move(value));

  uint64_t idx =
      0;  // position info , 这个主要用来判断是否是最后一个要查找的char，如果是最后一个，需要申请的是带有值的Node
  for (auto i : key) {
    std::shared_ptr<TrieNode> temp;
    auto v = node->children_.find(i);
    if (v != node->children_.end()) {
      // node exists
      if (idx + 1 == key.size()) {
        temp = std::make_shared<TrieNodeWithValue<T>>(v->second->children_, value_ptr);
      } else {
        temp = std::shared_ptr<TrieNode>(v->second->Clone());
      }
    } else {
      // node does not exist
      if (idx + 1 == key.size()) {
        temp = std::make_shared<TrieNodeWithValue<T>>(value_ptr);
      } else {
        temp = std::make_shared<TrieNode>();
      }
    }
    node->children_[i] = temp;
    node = temp;
    idx++;
  }

  if (node == root) {
    root = std::make_shared<TrieNodeWithValue<T>>(this->GetRoot()->children_, value_ptr);
  }

  Trie trie{root};
  return trie;

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  //  throw NotImplementedException("Trie::Remove is not implemented.");

  if (this->GetRoot() == nullptr) {
    return Trie{nullptr};
  }

  if (key.empty()) {
    // remove 根的情况
    if (this->GetRoot()->children_.empty()) {
      // 如果根没有children，删除
      return Trie{nullptr};
    }
    return Trie{std::make_shared<TrieNode>(this->GetRoot()->children_)};
  }

  auto new_root = this->GetRoot()->Clone();
  auto root = std::shared_ptr<TrieNode>(std::move(new_root));

  auto node = root;
  std::vector<std::shared_ptr<const TrieNode>> node_list{};

  bool node_is_found = true;

  for (auto i : key) {
    std::shared_ptr<TrieNode> temp;
    auto v = node->children_.find(i);
    if (v != node->children_.end()) {
      // node exists
      temp = std::shared_ptr<TrieNode>(v->second->Clone());
      node_list.push_back(temp);
    } else {
      // node does not exist
      node_is_found = false;
      break;
    }
    node->children_[i] = temp;
    node = temp;
  }
  if (node_is_found) {
    node = std::make_shared<TrieNode>(node->children_);

    // 获得最后一个节点的指针，并且修改其指向新建的节点node
    node_list.pop_back();
    auto it = node_list.rbegin();
    auto node_parent_ptr = const_cast<TrieNode *>((*it).get());
    node_parent_ptr->children_[key.back()] = node;

    if (node->children_.empty()) {
      int delete_count = 1;
      for (; it != node_list.rend(); ++it) {
        if ((*it)->children_.size() == 1 && !(*it)->is_value_node_ && (*it) != root) {
          delete_count += 1;
        } else {
          break;
        }
      }
      TrieNode *parent_ptr = nullptr;
      if (it != node_list.rend()) {
        parent_ptr = const_cast<TrieNode *>((*it).get());
      } else {
        // node_list遍历完全
        if (!root->is_value_node_ && root->children_.size() == 1) {
          // 如果根没有值，且根只有一个孩子（且遍历完全），则需要把根也变成nullptr
          return Trie{nullptr};
        }
        parent_ptr = root.get();
      }
      parent_ptr->children_.erase(key.at(key.size() - delete_count));
    }
  }
  return Trie{root};

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
