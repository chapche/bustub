//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *bpm, ReadPageGuard &&that, int index);
  IndexIterator(IndexIterator &&) noexcept = default;
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (index_ != itr.index_) {
      return false;
    }
    if (index_ < 0) {
      return true;
    }
    return guard_.GetData() == itr.guard_.GetData();
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    if (index_ == itr.index_ && index_ < 0) {
      return false;
    }
    return index_ != itr.index_ || guard_.GetData() != itr.guard_.GetData();
  }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_{nullptr};
  mutable ReadPageGuard guard_{};
  int index_{0};
};

}  // namespace bustub
