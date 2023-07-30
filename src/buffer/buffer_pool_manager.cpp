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
#include "common/logger.h"
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

// use latch to protect buffer pool
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // check if bufferpool is full
  std::lock_guard<std::mutex> guard(latch_);
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
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_id >= next_page_id_) {
    LOG_DEBUG("Fetch Non Existed Page page_id %d, next_page_id %d", (int)page_id, (int)next_page_id_);
    return nullptr;
  }
  frame_id_t frame_id;
  bool is_old_page = false;
  bool is_first_fetch = true;
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    is_first_fetch = false;
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
  }
  page->page_id_ = page_id;
  if (is_first_fetch) {
    disk_manager_->ReadPage(page->page_id_, page->GetData());
  }
  page->pin_count_++;
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  return pages_ + frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_DEBUG("Page_id Not Fould %d", (int)page_id);
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  if (page->pin_count_ == 0) {
    LOG_DEBUG("Unpin pin_count is Zero page_id %d", (int)page_id);
    return false;
  }
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  if (is_dirty && !page->is_dirty_) {
    page->is_dirty_ = true;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  disk_manager_->WritePage(page->page_id_, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> guard(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    auto page = pages_ + i;
    disk_manager_->WritePage(page->page_id_, page->GetData());
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  bool is_count_zero = page->pin_count_ == 0;
  if (!is_count_zero) {
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->GetData());
    page->is_dirty_ = false;
  }
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  page->pin_count_ = 0;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
