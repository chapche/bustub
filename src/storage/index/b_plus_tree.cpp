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
      leaf_max_size_(leaf_max_size),  // FIXME: what if max_size is bigger than LEAF_PAGE_SIZE ?
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.template AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.template As<BPlusTreeHeaderPage>();
  if (nullptr != header_page && header_page->root_page_id_ != INVALID_PAGE_ID) {
    auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
    auto root_page = root_page_guard.template As<BPlusTreePage>();
    if (nullptr != root_page) {
      return root_page->GetSize() == 0;
    }
  }
  return true;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  // Context ctx;
  // (void)ctx;
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.template As<BPlusTreeHeaderPage>();
  if (nullptr == header_page || header_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  auto start_page_id = header_page->root_page_id_;
  guard.Drop();
  do {
    auto page_guard = bpm_->FetchPageRead(start_page_id);
    auto page = page_guard.template As<BPlusTreePage>();
    if (nullptr == page) {
      return false;
    }
    // use binary search to avoid overtime
    auto size = page->GetSize();
    if (0 == size) {
      return false;
    }
    if (page->IsLeafPage()) {
      auto leaf_page = reinterpret_cast<const LeafPage *>(page);
      if (nullptr == leaf_page) {
        return false;
      }
      int right = size - 1;
      int left = 0;
      int mid = left;
      while (left <= right) {
        mid = left + (right - left) / 2;
        const auto page_key = leaf_page->KeyAt(mid);
        if (comparator_(key, page_key) >= 0) {
          if (0 == comparator_(key, page_key)) {
            result->push_back(leaf_page->ValueAt(mid));
            return true;
          }
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }
      // LOG_WARN("Value Not Found! last index: %d", mid);
      break;
    }
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    if (nullptr == internal_page) {
      return false;
    }
    if (size <= 1) {
      return false;
    }
    int right = size - 1;
    int left = 1;
    int mid = left;
    while (left <= right) {
      mid = left + (right - left) / 2;
      const auto page_key = internal_page->KeyAt(mid);
      if (comparator_(key, page_key) > 0) {
        left = mid + 1;
      } else {
        right = mid - 1;
        mid -= 1;
      }
    }
    start_page_id = internal_page->ValueAt(mid);
  } while (start_page_id != INVALID_PAGE_ID);
  return false;
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
  // Declaration of context instance.
  Context ctx;
  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.template AsMut<BPlusTreeHeaderPage>();
  if (nullptr == header_page) {
    return false;
  }
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    // init first page
    auto root_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
    auto page = root_guard.template AsMut<LeafPage>();
    page->SetMaxSize(leaf_max_size_);
    page->SetPageType(IndexPageType::LEAF_PAGE);
    page->SetSize(0);
    page->SetNextPageId(INVALID_PAGE_ID);
  }
  auto start_page_id = header_page->root_page_id_;
  ctx.root_page_id_ = header_page->root_page_id_;
  guard.Drop();
  do {
    ctx.write_set_.push_back(bpm_->FetchPageWrite(start_page_id));
    auto page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
    if (nullptr == page) {
      return false;
    }
    auto size = page->GetSize();
    if (size > 3 && size >= page->GetMaxSize()) {  // split before insertion for internal page
      DoSplit(ctx);
      page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
    }
    if (nullptr == page) {
      return false;
    }
    // judge if this node will split;if not, we could release the latch of grandparent or above beforehand
    while (ctx.write_set_.size() > 2 && size < page->GetMaxSize()) {
      auto &tmp_guard = ctx.write_set_.front();
      tmp_guard.Drop();
      ctx.write_set_.pop_front();
    }
    size = page->GetSize();
    if (page->IsLeafPage()) {
      auto leaf_page = reinterpret_cast<LeafPage *>(page);
      int left = 0;
      int right = size > 0 ? size - 1 : 0;
      int mid = left;
      while (size > 0 && left <= right) {  // lower_bound
        mid = left + (right - left) / 2;
        const auto page_key = leaf_page->KeyAt(mid);
        if (comparator_(key, page_key) >= 0) {
          if (0 == comparator_(key, page_key)) {  // duplicate key
            return false;
          }
          left = mid + 1;
        } else if (0 > comparator_(key, page_key)) {
          right = mid - 1;
        }
      }
      if (size > 0) {  // avoid UB if size is zero
        const auto page_key = leaf_page->KeyAt(mid);
        if (0 > comparator_(key, page_key)) {
          leaf_page->InsertAt(mid, {key, value});
        } else {
          leaf_page->InsertAt(mid + 1, {key, value});
        }
      } else {
        leaf_page->InsertAt(mid, {key, value});
      }
      size = leaf_page->GetSize();
      if (size >= leaf_max_size_) {
        DoSplit(ctx);
      }
      return true;
    }
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    if (nullptr == internal_page) {
      return false;
    }
    if (size <= 1) {
      return false;
    }
    int right = size - 1;
    int left = 1;
    int mid = left;
    while (left <= right) {
      mid = left + (right - left) / 2;
      const auto page_key = internal_page->KeyAt(mid);
      if (comparator_(key, page_key) > 0) {
        left = mid + 1;
      } else if (0 >= comparator_(key, page_key)) {
        right = mid - 1;
        mid -= 1;
      }
    }
    start_page_id = internal_page->ValueAt(mid);
  } while (start_page_id != INVALID_PAGE_ID);
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.template AsMut<BPlusTreeHeaderPage>();
  if (nullptr == header_page) {
    return;
  }
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  auto start_page_id = header_page->root_page_id_;
  ctx.root_page_id_ = header_page->root_page_id_;
  guard.Drop();
  do {
    ctx.write_set_.push_back(bpm_->FetchPageWrite(start_page_id));
    auto page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
    if (nullptr == page) {
      return;
    }
    auto size = page->GetSize();
    if (size > 3 && size >= page->GetMaxSize()) {  // split before insertion for internal page
      DoSplit(ctx);
      page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
    }
    size = page->GetSize();
    if (page->IsLeafPage()) {
      auto leaf_page = reinterpret_cast<LeafPage *>(page);
      int left = 0;
      int right = size > 0 ? size - 1 : 0;
      int mid = left;
      while (size > 0 && left <= right) {  // lower_bound
        mid = left + (right - left) / 2;
        const auto page_key = leaf_page->KeyAt(mid);
        if (comparator_(key, page_key) >= 0) {
          if (0 == comparator_(key, page_key)) {
            leaf_page->RemoveAt(mid);
            DoMerge(ctx);
            return;
          }
          left = mid + 1;
        } else if (0 > comparator_(key, page_key)) {
          right = mid - 1;
        }
      }
      return;
    }
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    if (nullptr == internal_page) {
      return;
    }
    if (size <= 1) {
      return;
    }
    int right = size - 1;
    int left = 1;
    int mid = left;
    while (left <= right) {
      mid = left + (right - left) / 2;
      const auto page_key = internal_page->KeyAt(mid);
      if (comparator_(key, page_key) > 0) {
        left = mid + 1;
      } else if (0 >= comparator_(key, page_key)) {
        right = mid - 1;
        mid -= 1;
      }
    }
    start_page_id = internal_page->ValueAt(mid);
  } while (start_page_id != INVALID_PAGE_ID);
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
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.template As<BPlusTreeHeaderPage>();
  if (nullptr == header_page || header_page->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }
  auto start_page_id = header_page->root_page_id_;
  guard.Drop();
  do {
    auto page_guard = bpm_->FetchPageRead(start_page_id);
    auto page = page_guard.template As<BPlusTreePage>();
    if (nullptr == page) {
      start_page_id = INVALID_PAGE_ID;
      break;
    }
    auto size = page->GetSize();
    if (page->IsLeafPage()) {
      break;
    }
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    if (nullptr == internal_page) {
      start_page_id = INVALID_PAGE_ID;
      break;
    }
    if (size <= 1) {
      start_page_id = INVALID_PAGE_ID;
      break;
    }
    start_page_id = internal_page->ValueAt(0);
  } while (start_page_id != INVALID_PAGE_ID);
  if (start_page_id == INVALID_PAGE_ID) {
    return End();
  }
  return INDEXITERATOR_TYPE(bpm_, bpm_->FetchPageRead(start_page_id), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.template As<BPlusTreeHeaderPage>();
  if (nullptr == header_page || header_page->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }
  auto start_page_id = header_page->root_page_id_;
  guard.Drop();
  int index = -1;
  do {
    auto page_guard = bpm_->FetchPageRead(start_page_id);
    auto page = page_guard.template As<BPlusTreePage>();
    if (nullptr == page) {
      break;
    }
    // use binary search to avoid overtime
    auto size = page->GetSize();
    if (0 == size) {
      break;
    }
    if (page->IsLeafPage()) {
      auto leaf_page = reinterpret_cast<const LeafPage *>(page);
      if (nullptr == leaf_page) {
        break;
      }
      int right = size - 1;
      int left = 0;
      int mid = left;
      while (left <= right) {
        mid = left + (right - left) / 2;
        const auto page_key = leaf_page->KeyAt(mid);
        if (comparator_(key, page_key) >= 0) {
          if (0 == comparator_(key, page_key)) {
            index = mid;
            break;
          }
          left = mid + 1;
        } else if (0 > comparator_(key, page_key)) {
          right = mid - 1;
        }
      }
      break;
    }
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    if (nullptr == internal_page) {
      break;
    }
    if (size <= 1) {
      break;
    }
    int right = size - 1;
    int left = 1;
    int mid = left;
    while (left <= right) {
      mid = left + (right - left) / 2;
      const auto page_key = internal_page->KeyAt(mid);
      if (comparator_(key, page_key) > 0) {
        left = mid + 1;
      } else if (0 >= comparator_(key, page_key)) {
        right = mid - 1;
        mid -= 1;
      }
    }
    start_page_id = internal_page->ValueAt(mid);
  } while (start_page_id != INVALID_PAGE_ID);
  if (index < 0) {
    return End();
  }
  return INDEXITERATOR_TYPE(bpm_, bpm_->FetchPageRead(start_page_id), index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, ReadPageGuard(bpm_, nullptr), -1); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.template As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*
 * Input is pageguard, traverse the path and do the split
 * @return : void
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DoSplit(Context &ctx) {  // Split once; Avoid holding latch for too long
  if (ctx.write_set_.empty()) {
    LOG_DEBUG("Splitting empty write_set");
    return;
  }
  auto page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto page = page_guard.template AsMut<BPlusTreePage>();
  // check size
  int size = page->GetSize();
  if (page->GetSize() < page->GetMaxSize()) {
    LOG_DEBUG("CurSize(%d) MaxSize(%d) No Need To Split", page->GetSize(), page->GetMaxSize());
    return;
  }
  // make sure root_page_id do not change to avoid gaining the latch of header_page
  if (ctx.IsRootPage(page_guard.PageId())) {
    // find the mid point to split
    if (page->IsLeafPage()) {
      auto leaf_page = reinterpret_cast<LeafPage *>(page);
      int mid = (size - 1) / 2;
      page_id_t left_page_id = INVALID_PAGE_ID;
      page_id_t right_page_id = INVALID_PAGE_ID;
      auto left_page_guard = bpm_->NewPageGuarded(&left_page_id);
      auto right_page_guard = bpm_->NewPageGuarded(&right_page_id);
      // init child page
      auto left_child_page = left_page_guard.template AsMut<LeafPage>();
      left_child_page->SetPageType(IndexPageType::LEAF_PAGE);
      left_child_page->SetMaxSize(leaf_max_size_);
      left_child_page->SetSize(mid + 1);
      left_child_page->SetNextPageId(right_page_guard.PageId());
      auto right_child_page = right_page_guard.template AsMut<LeafPage>();
      right_child_page->SetPageType(IndexPageType::LEAF_PAGE);
      right_child_page->SetMaxSize(leaf_max_size_);
      right_child_page->SetSize(size - mid - 1);
      right_child_page->SetNextPageId(INVALID_PAGE_ID);
      auto mid_key = leaf_page->KeyAt(mid);
      for (int i = 0; i <= mid; i++) {
        left_child_page->SetMappingAt(i, leaf_page->MappingAt(i));
      }
      for (int i = mid + 1; i < size; i++) {
        right_child_page->SetMappingAt(i - mid - 1, leaf_page->MappingAt(i));
      }
      auto leaf_internal_page = reinterpret_cast<InternalPage *>(page);
      leaf_internal_page->SetPageType(IndexPageType::INTERNAL_PAGE);
      leaf_internal_page->SetMappingAt(0, {KeyType{}, left_page_guard.PageId()});
      leaf_internal_page->SetMappingAt(1, {mid_key, right_page_guard.PageId()});
      leaf_internal_page->SetMaxSize(internal_max_size_);
      leaf_internal_page->SetSize(2);
    } else {
      auto internal_page = reinterpret_cast<InternalPage *>(page);
      int mid = size / 2;
      page_id_t left_page_id = INVALID_PAGE_ID;
      page_id_t right_page_id = INVALID_PAGE_ID;
      auto left_page_guard = bpm_->NewPageGuarded(&left_page_id);
      auto right_page_guard = bpm_->NewPageGuarded(&right_page_id);
      // init child page
      auto left_child_page = left_page_guard.template AsMut<InternalPage>();
      left_child_page->SetPageType(IndexPageType::INTERNAL_PAGE);
      left_child_page->SetMaxSize(internal_max_size_);
      left_child_page->SetSize(mid);
      auto right_child_page = right_page_guard.template AsMut<InternalPage>();
      right_child_page->SetPageType(IndexPageType::INTERNAL_PAGE);
      right_child_page->SetMaxSize(internal_max_size_);
      right_child_page->SetSize(size - mid);
      auto mid_key = internal_page->KeyAt(mid);
      for (int i = 0; i < mid; i++) {
        left_child_page->SetMappingAt(i, internal_page->MappingAt(i));
      }
      right_child_page->SetMappingAt(0, {KeyType{}, internal_page->ValueAt(mid)});
      for (int i = mid + 1; i < size; i++) {
        right_child_page->SetMappingAt(i - mid, internal_page->MappingAt(i));
      }
      internal_page->SetMappingAt(0, {KeyType{}, left_page_guard.PageId()});
      internal_page->SetMappingAt(1, {mid_key, right_page_guard.PageId()});
      internal_page->SetSize(2);
      // we should regain the lock because we might still be traversing
    }
    ctx.write_set_.push_back(std::move(page_guard));
  } else {
    if (page->IsLeafPage()) {
      auto leaf_page = reinterpret_cast<LeafPage *>(page);
      if (ctx.write_set_.empty()) {
        LOG_WARN("Split non-root leaf, Not Hava Parent Latch!");
        return;
      }
      auto parent_page_guard = std::move(ctx.write_set_.back());
      ctx.write_set_.pop_back();
      auto parent_page = parent_page_guard.template AsMut<InternalPage>();
      int parent_size = parent_page->GetSize();
      if (parent_size < 2) {
        LOG_WARN("Empty parent Node!");
        return;
      }
      int mid = (size - 1) / 2;
      auto mid_key = leaf_page->KeyAt(mid);
      page_id_t right_page_id = INVALID_PAGE_ID;
      auto right_page_guard = bpm_->NewPageGuarded(&right_page_id);
      if (INVALID_PAGE_ID == right_page_id) {
        LOG_WARN("NewPageGuarded Failed DoSplit Failed");
        return;
      }
      // leaf_page turns into left page
      // init right child page
      auto right_child_page = right_page_guard.template AsMut<LeafPage>();
      right_child_page->SetPageType(IndexPageType::LEAF_PAGE);
      right_child_page->SetMaxSize(leaf_max_size_);
      right_child_page->SetSize(size - mid - 1);
      right_child_page->SetNextPageId(leaf_page->GetNextPageId());
      leaf_page->SetNextPageId(right_page_guard.PageId());
      for (int i = mid + 1; i < size; i++) {
        right_child_page->SetMappingAt(i - mid - 1, leaf_page->MappingAt(i));
      }
      // insert mid_key into parent node
      int index = 1;
      int left = 1;
      int right = parent_size - 1;
      while (left <= right) {  // lower_bound
        index = left + (right - left) / 2;
        auto key = parent_page->KeyAt(index);
        if (comparator_(key, mid_key) > 0) {
          right = index - 1;
        } else if (0 >= comparator_(key, mid_key)) {
          left = index + 1;
        }
      }
      auto page_key = parent_page->KeyAt(index);
      if (0 > comparator_(page_key, mid_key)) {
        index += 1;
      }
      parent_page->InsertAt(index, {mid_key, right_page_guard.PageId()});
      // clean up left child
      leaf_page->SetSize(mid + 1);
      ctx.write_set_.push_back(std::move(parent_page_guard));
    } else {
      auto internal_page = reinterpret_cast<InternalPage *>(page);
      if (ctx.write_set_.empty()) {
        LOG_WARN("Split non-root leaf, Not Hava Parent Latch!");
        return;
      }
      auto parent_page_guard = std::move(ctx.write_set_.back());
      ctx.write_set_.pop_back();
      auto parent_page = parent_page_guard.template AsMut<InternalPage>();
      int parent_size = parent_page->GetSize();
      if (parent_size < 2) {
        LOG_WARN("Empty parent Node!");
        return;
      }
      int mid = size / 2;
      auto mid_key = internal_page->KeyAt(mid);
      page_id_t right_page_id = INVALID_PAGE_ID;
      auto right_page_guard = bpm_->NewPageGuarded(&right_page_id);
      // leaf_page turns into left page
      // init right child page
      auto right_child_page = right_page_guard.template AsMut<InternalPage>();
      right_child_page->SetPageType(IndexPageType::INTERNAL_PAGE);
      right_child_page->SetMaxSize(internal_max_size_);
      right_child_page->SetSize(size - mid);
      right_child_page->SetMappingAt(0, {KeyType{}, internal_page->ValueAt(mid)});
      for (int i = mid + 1; i < size; i++) {
        right_child_page->SetMappingAt(i - mid, internal_page->MappingAt(i));
      }
      internal_page->SetSize(mid);
      // insert mid_key into parent node
      int index = 1;
      int left = 1;
      int right = parent_size - 1;
      while (left <= right) {  // lower_bound
        index = left + (right - left) / 2;
        auto key = parent_page->KeyAt(index);
        if (comparator_(key, mid_key) > 0) {
          right = index - 1;
          index = right;
        } else if (0 >= comparator_(key, mid_key)) {
          left = index + 1;
        }
      }
      auto page_key = parent_page->KeyAt(index);
      if (0 > comparator_(page_key, mid_key)) {
        index += 1;
      }
      parent_page->InsertAt(index, {mid_key, right_page_guard.PageId()});
      ctx.write_set_.push_back(std::move(parent_page_guard));
    }
  }
}

/*
 * Input is pageguard, traverse the path and do the merge
 * @return : void
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DoMerge(Context &ctx) {
  if (ctx.write_set_.empty()) {
    LOG_DEBUG("Merging empty write_set");
    return;
  }
  auto page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto page = page_guard.template AsMut<BPlusTreePage>();
  // check size
  int size = page->GetSize();
  // Note: if node is half-full, no need rebalancing
  // Internal node size must greater than 1!
  if ((page->IsLeafPage() || size > 1) && size >= page->GetMinSize()) {
    return;
  }

  while (!ctx.write_set_.empty()) {
    if ((page->IsLeafPage() || page->GetSize() > 1) && page->GetSize() >= page->GetMinSize()) {
      LOG_DEBUG("no need to merge");
      return;
    }
    auto parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto parent_page = parent_guard.template AsMut<InternalPage>();
    int parent_size = parent_page->GetSize();
    int index = parent_page->ValueIndex(page_guard.PageId());
    if (index < 0) {
      LOG_WARN("PageId Not Found");
      return;
    }
    if (page->IsLeafPage()) {
      if (index > 0) {
        // try borrow node from left child
        // must be careful , acquiring leaf sibling lock might cause deadlock
        // Regaining lock from left to right
        page_guard.Drop();
        auto left_child_page_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index - 1));
        page_guard = std::move(bpm_->FetchPageWrite(parent_page->ValueAt(index)));
        auto left_child_page = left_child_page_guard.template AsMut<LeafPage>();
        auto self_page = page_guard.template AsMut<LeafPage>();
        int left_size = left_child_page->GetSize();
        size = self_page->GetSize();
        int total_size = left_size + size;
        if (total_size >= left_child_page->GetMaxSize()) {  // right rotate
          int move_num = self_page->GetMinSize() - size;
          if (0 == move_num) {
            LOG_WARN("Nothing to move!");
            return;
          }
          // set new max left child's key at parent
          parent_page->SetKeyAt(index, left_child_page->KeyAt(left_size - move_num - 1));
          self_page->SetSize(size + move_num);
          for (int i = size - 1; i >= 0; i--) {  // update right child node
            self_page->SetMappingAt(i + move_num, self_page->MappingAt(i));
          }
          for (int i = 0; i < move_num; i++) {  // copy to
            self_page->SetMappingAt(i, left_child_page->MappingAt(i + left_size - move_num));
          }
          left_child_page->SetSize(left_size - move_num);
          return;
        }
        // Not enouph node, then merge child and make parent empty
        // if parent is root, just copy to root and avoiding fetching header page's latch
        if (parent_size < 3 && ctx.IsRootPage(parent_guard.PageId())) {
          auto root_page = parent_guard.template AsMut<LeafPage>();
          for (int i = 0; i < left_size; i++) {
            root_page->SetMappingAt(i, left_child_page->MappingAt(i));
          }
          for (int i = 0; i < size; i++) {
            root_page->SetMappingAt(i + left_size, self_page->MappingAt(i));
          }
          root_page->SetPageType(IndexPageType::LEAF_PAGE);
          root_page->SetSize(left_size + size);
          root_page->SetMaxSize(left_child_page->GetMaxSize());
          root_page->SetNextPageId(INVALID_PAGE_ID);
          return;
        }
        for (int i = 0; i < size; i++) {
          left_child_page->SetMappingAt(i + left_size, self_page->MappingAt(i));
        }
        left_child_page->SetNextPageId(self_page->GetNextPageId());
        left_child_page->SetSize(left_size + size);
        parent_page->RemoveAt(index);
        if (parent_page->GetSize() > 1) {  // No need to propogate to root!
          return;
        }
      } else if (0 == index) {  // try right sibling
        auto right_child_page_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index + 1));
        auto right_child_page = right_child_page_guard.template AsMut<LeafPage>();
        auto self_page = page_guard.template AsMut<LeafPage>();
        int right_size = right_child_page->GetSize();
        size = self_page->GetSize();
        if (right_size + size >= right_child_page->GetMaxSize()) {
          int move_num = self_page->GetMinSize() - size;
          if (0 == move_num) {
            LOG_WARN("Nothing to move!");
            return;
          }
          // set new max left child's key at parent
          parent_page->SetKeyAt(index + 1, right_child_page->KeyAt(move_num - 1));
          self_page->SetSize(size + move_num);
          for (int i = 0; i < move_num; i++) {
            self_page->SetMappingAt(i + size, right_child_page->MappingAt(i));
          }
          for (int i = 0; i < right_size - move_num; i++) {
            right_child_page->SetMappingAt(i, right_child_page->MappingAt(i + move_num));
          }
          right_child_page->SetSize(right_size - move_num);
          return;
        }
        // Not enouph node, then merge child and make parent empty
        // if parent is root, just copy to root and avoiding fetching header page's latch
        if (parent_size < 3 && ctx.IsRootPage(parent_guard.PageId())) {
          auto root_page = parent_guard.template AsMut<LeafPage>();
          for (int i = 0; i < size; i++) {
            root_page->SetMappingAt(i, self_page->MappingAt(i));
          }
          for (int i = 0; i < right_size; i++) {
            root_page->SetMappingAt(i + size, right_child_page->MappingAt(i));
          }
          root_page->SetPageType(IndexPageType::LEAF_PAGE);
          root_page->SetSize(right_size + size);
          root_page->SetMaxSize(self_page->GetMaxSize());
          root_page->SetNextPageId(INVALID_PAGE_ID);
          return;
        }
        for (int i = 0; i < right_size; i++) {
          self_page->SetMappingAt(i + size, right_child_page->MappingAt(i));
        }
        self_page->SetNextPageId(right_child_page->GetNextPageId());
        self_page->SetSize(size + right_size);
        parent_page->RemoveAt(index + 1);
        if (parent_page->GetSize() > 1) {  // No need to propogate to root!
          // LOG_DEBUG("No Need To Propagate to Root size : %d", parent_page->GetSize());
          return;
        }
      }
    } else {  // Internal Node
      if (index > 0) {
        // merge to left_child_page and make parent empty!
        auto left_child_page_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index - 1));
        auto left_child_page = left_child_page_guard.template AsMut<InternalPage>();
        auto self_page = page_guard.template AsMut<InternalPage>();
        int left_size = left_child_page->GetSize();
        size = self_page->GetSize();
        // try to borrow node from sibling i.e. Right Rotating
        if (left_size > 2 && left_size + size >= left_child_page->GetMaxSize()) {
          int move_num = self_page->GetMinSize() > 2 ? self_page->GetMinSize() : 2 - size;
          if (0 == move_num) {
            LOG_WARN("Nothing to move");
            return;
          }
          // set new max left child's key at parent
          auto parent_key = parent_page->KeyAt(index);
          parent_page->SetKeyAt(index, left_child_page->KeyAt(left_size - move_num));
          self_page->SetSize(size + move_num);
          for (int i = size - 1; i >= 0; i--) {
            self_page->SetMappingAt(i + move_num, self_page->MappingAt(i));
          }

          self_page->SetKeyAt(move_num, parent_key);
          for (int i = 0; i < move_num; i++) {  // copy to
            self_page->SetMappingAt(i, left_child_page->MappingAt(i + left_size - move_num));
          }
          self_page->SetKeyAt(0, KeyType{});
          left_child_page->SetSize(left_size - move_num);
          return;
        }
        // if parent is root, just copy to root and avoiding fetching header page's latch
        if (parent_size < 3 && ctx.IsRootPage(parent_guard.PageId())) {
          auto parent_key = parent_page->KeyAt(1);
          for (int i = 0; i < left_size; i++) {
            parent_page->SetMappingAt(i, left_child_page->MappingAt(i));
          }
          for (int i = 0; i < size; i++) {
            parent_page->SetMappingAt(i + left_size, self_page->MappingAt(i));
          }
          parent_page->SetKeyAt(0, KeyType{});
          parent_page->SetKeyAt(left_size, parent_key);
          parent_page->SetPageType(IndexPageType::INTERNAL_PAGE);
          parent_page->SetSize(left_size + size);
          return;
        }
        // we cannot borrow node from sibling, then merge with left and make parent empty and propagate to root
        // merge with left child
        left_child_page->SetMappingAt(left_size, {parent_page->KeyAt(index), self_page->ValueAt(0)});
        for (int i = 1; i < size; i++) {  // copy remaining node to left child
          left_child_page->SetMappingAt(i + left_size, self_page->MappingAt(i));
        }
        // parent_page->SetMappingAt(index - 1, {parent_page->KeyAt(index - 1), left_child_page->ValueAt(0)});
        left_child_page->SetSize(left_size + size);
        parent_page->RemoveAt(index);
        if (parent_page->GetSize() > 1) {  // DO NOT propagate to root!
          return;
        }
      } else if (0 == index) {  // try right sibling
        auto right_child_page_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index + 1));
        auto right_child_page = right_child_page_guard.template AsMut<InternalPage>();
        auto self_page = page_guard.template AsMut<InternalPage>();
        int right_size = right_child_page->GetSize();
        size = self_page->GetSize();
        if (right_size > 2 && right_size + size >= right_child_page->GetMaxSize()) {
          int move_num = self_page->GetMinSize() > 2 ? self_page->GetMinSize() : 2 - size;
          if (0 == move_num) {
            LOG_WARN("Nothing to move!");
            return;
          }
          // do left rotate
          self_page->SetMappingAt(size, {parent_page->KeyAt(index + 1), right_child_page->ValueAt(0)});
          parent_page->SetKeyAt(index + 1, right_child_page->KeyAt(move_num));
          self_page->SetSize(size + move_num);
          for (int i = 1; i < move_num; i++) {
            self_page->SetMappingAt(i + size, right_child_page->MappingAt(i));
          }
          for (int i = 0; i < right_size - move_num; i++) {
            right_child_page->SetMappingAt(i, right_child_page->MappingAt(i + move_num));
          }
          right_child_page->SetKeyAt(0, KeyType{});
          right_child_page->SetSize(right_size - move_num);
          return;
        }
        // if parent is root, just copy to root and avoiding fetching header page's latch
        if (parent_size < 3 && ctx.IsRootPage(parent_guard.PageId())) {
          auto parent_key = parent_page->KeyAt(1);
          for (int i = 0; i < size; i++) {
            parent_page->SetMappingAt(i, self_page->MappingAt(i));
          }
          for (int i = 0; i < right_size; i++) {
            parent_page->SetMappingAt(i + size, right_child_page->MappingAt(i));
          }
          parent_page->SetKeyAt(0, KeyType{});
          parent_page->SetKeyAt(size, parent_key);
          parent_page->SetPageType(IndexPageType::INTERNAL_PAGE);
          parent_page->SetSize(right_size + size);
          return;
        }
        // merge to left_child and propagate to root!
        self_page->SetMappingAt(size, {parent_page->KeyAt(index + 1), right_child_page->ValueAt(0)});
        for (int i = 1; i < right_size; i++) {
          self_page->SetMappingAt(i + size, right_child_page->MappingAt(i));
        }
        self_page->SetSize(size + right_size);
        parent_page->RemoveAt(index + 1);
        if (parent_page->GetSize() > 1) {  // No need to propogate to root!
          // LOG_DEBUG("No Need To Propagate to Root size : %d", parent_page->GetSize());
          return;
        }
      }
    }
    page_guard = std::move(parent_guard);
    page = page_guard.template AsMut<BPlusTreePage>();
  }
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
    LOG_WARN("Drawing an empty tree");
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
