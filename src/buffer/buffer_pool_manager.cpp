//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

// FIXME: use latch to protect buffer pool
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // check if bufferpool is full
  frame_id_t new_frame = 0;
  bool is_old_page = false;
  if (free_list_.empty()) {
    // Evict a page from replacer
    bool res = replacer_->Evict(&new_frame);
    if (!res) {  // failed to release any page
      return nullptr;
    }
    is_old_page = true;
  } else {
    // get a page from freelist
    new_frame = free_list_.back();
    free_list_.pop_back();
  }
  // insert into page_table_
  auto new_page_id = AllocatePage();
  *page_id = new_page_id;
  page_table_[new_page_id] = new_frame;
  replacer_->SetEvictable(new_frame, false);
  replacer_->RecordAccess(new_frame);
  auto page = pages_ + new_frame;
  page->WLatch();
  if (is_old_page) {
    // do some cleanup
    if (page->page_id_ != INVALID_PAGE_ID && page->page_id_ != new_page_id) {
      page_table_.erase(page->page_id_);
    }
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->GetData());
      page->is_dirty_ = false;
    }
  }
  page->page_id_ = new_page_id;
  page->pin_count_ = 1;
  page->ResetMemory();
  page->WUnlatch();
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t frame_id;
  bool is_old_page = false;
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
  } else {
    if (free_list_.empty()) {
      bool res = replacer_->Evict(&frame_id);
      if (!res) {
        return nullptr;
      }
      is_old_page = true;
    } else {
      frame_id = free_list_.back();
      free_list_.pop_back();
    }
    page_table_[page_id] = frame_id;
  }
  auto page = pages_ + frame_id;
  page->WLatch();
  if (is_old_page) {
    // do some cleanup
    if (page->page_id_ != INVALID_PAGE_ID && page->page_id_ != page_id) {
      page_table_.erase(page->page_id_);
    }
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->GetData());
      page->is_dirty_ = false;
    }
    page->pin_count_ = 0;
    page->page_id_ = page_id;
    disk_manager_->ReadPage(page->page_id_, page->GetData());
  }
  page->pin_count_++;
  page->WUnlatch();
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  return pages_ + frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  page->RLatch();
  bool is_count_zero = page->pin_count_ == 0;
  page->RUnlatch();
  if (is_count_zero) {
    return false;
  }
  page->WLatch();
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  if (is_dirty && false == page->is_dirty_) {
    page->is_dirty_ = true;
  }
  page->WUnlatch();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  page->WLatch();
  disk_manager_->WritePage(page->page_id_, page->GetData());
  page->is_dirty_ = false;
  page->WUnlatch();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < pool_size_; ++i) {
    auto page = pages_ + i;
    page->WLatch();
    disk_manager_->WritePage(page->page_id_, page->GetData());
    page->is_dirty_ = false;
    page->WUnlatch();
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  page->RLatch();
  bool is_count_zero = page->pin_count_ == 0;
  page->RUnlatch();
  if (!is_count_zero) {
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  page->WLatch();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->ResetMemory();
  page->WUnlatch();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
