#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  if (this != &that) {
    Steal(that);
  }
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop();
    Steal(that);
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  if (this != &that) {
    guard_.Steal(that.guard_);
  }
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    // release myself first
    Drop();
    guard_.Steal(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
    guard_.page_->RUnlatch();
  }
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
  guard_.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  if (this != &that) {
    guard_.Steal(that.guard_);
  }
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    guard_.Steal(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
    guard_.page_->WUnlatch();
  }
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
  guard_.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
