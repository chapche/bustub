/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard &&that, int index)
    : bpm_(bpm), guard_(std::move(that)), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (index_ < 0) {
    return true;
  }
  auto leaf_page = guard_.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  if (nullptr == leaf_page) {
    return true;
  }
  if (INVALID_PAGE_ID != leaf_page->GetNextPageId()) {
    return false;
  }
  int size = leaf_page->GetSize();
  return size == 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto leaf_page = guard_.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  // if (nullptr == leaf_page) {  // shall we abort ?
  //     return MappingType{};
  // }
  // int size = leaf_page->GetSize();
  // if (index_ >= size) {
  //     return MappingType{};
  // }
  return leaf_page->MappingAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto leaf_page = guard_.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  if (nullptr == leaf_page) {  // shall we abort ?
    index_ = -1;
    return *this;
  }
  int size = leaf_page->GetSize();
  index_++;
  if (index_ < size) {
    return *this;
  }
  auto next_page_id = leaf_page->GetNextPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    index_ = -1;
    guard_ = ReadPageGuard(bpm_, nullptr);
  } else {
    guard_ = bpm_->FetchPageRead(next_page_id);
    index_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
