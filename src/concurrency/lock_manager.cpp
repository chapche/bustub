//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <set>
#include <stack>

#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (!CanTxnTakeLock(txn, lock_mode)) {
    LOG_DEBUG("txn cannot take lock txn_id: %d table_oid: %u", txn->GetTransactionId(), oid);
    return false;
  }
  auto txn_id = txn->GetTransactionId();
  std::unique_lock<std::mutex> g(table_lock_map_latch_);
  // fresh lock
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto q = table_lock_map_[oid];
  // upgrading should be proritised over other waiting lock requests
  bool is_compatible = true;
  auto lock_request = GetLockRequest(q.get(), lock_mode, txn_id, is_compatible);
  if (lock_request != nullptr) {
    if (lock_request->lock_mode_ == lock_mode && lock_request->granted_) {
      return true;
    }
    // check if there is any txn upgrading
    if (INVALID_TXN_ID != q->upgrading_) {  // do aborting UPGRADE_CONFLICT
      LOG_DEBUG("do aborting UPGRADE_CONFLICT upgrading_txn:%d txn_id: %d", q->upgrading_, txn_id);
      txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
    }
    if (CanLockUpgrade(lock_request->lock_mode_, lock_mode)) {
      q->upgrading_ = txn_id;
      lock_request->granted_ = false;
      DeleteTxnTableLockSet(txn, lock_request->lock_mode_, oid);
      lock_request->lock_mode_ = lock_mode;
    } else {
      LOG_DEBUG("do aborting INCOMPATIBLE_UPGRADE");
      txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
    }
  } else {
    lock_request = new LockRequest(txn_id, lock_mode, oid);
    q->request_queue_.push_back(lock_request);
  }
  // we shall wait
  while (!is_compatible) {
    // LOG_DEBUG("NOT Compatible keep waiting txn_id: %d table_oid: %u", txn_id, oid);
    q->cv_.wait(g);
    if (!CanTxnTakeLock(txn, lock_mode)) {  // do some cleanup
      auto iterator = q->request_queue_.begin();
      for (; iterator != q->request_queue_.end() && (*iterator)->txn_id_ != txn_id; ++iterator) {
      }
      if (iterator != q->request_queue_.end()) {
        q->request_queue_.erase(iterator);
      }
      if (txn_id == q->upgrading_) {
        q->upgrading_ = INVALID_TXN_ID;
      }
      delete lock_request;
      q->cv_.notify_all();
      return false;
    }
    is_compatible = true;
    lock_request = GetLockRequest(q.get(), lock_mode, txn_id, is_compatible);
    // LOG_DEBUG("NOT Compatible After waiting txn_id: %d table_oid: %u", txn_id, oid);
  }
  lock_request->granted_ = true;
  if (txn_id == q->upgrading_) {
    q->upgrading_ = INVALID_TXN_ID;
  }
  // LOG_DEBUG("Granted Lock txn_id: %d table_oid: %u", txn_id, oid);
  UpdateTxnTableLockSet(txn, lock_mode, oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // find if we hold the lock
  auto txn_id = txn->GetTransactionId();
  // LOG_DEBUG("Before notify txn_id: %d, table_oid: %u", txn_id, oid);
  std::unique_lock<std::mutex> g(table_lock_map_latch_);
  auto q = table_lock_map_[oid];
  if (nullptr == q) {
    LOG_DEBUG("do aborting ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD");
    txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto iterator = q->request_queue_.begin();
  bool is_found = false;
  for (; iterator != q->request_queue_.end(); ++iterator) {
    if ((*iterator)->txn_id_ == txn_id && (*iterator)->granted_) {
      is_found = true;
      break;
    }
  }
  if (!is_found) {
    LOG_DEBUG("do aborting ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD");
    txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if (!(*s_row_lock_set)[oid].empty() || !(*x_row_lock_set)[oid].empty()) {
    LOG_DEBUG("do aborting TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS");
    txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  auto lock_request = *iterator;
  ChangeTxnState(txn, lock_request->lock_mode_);
  q->request_queue_.erase(iterator);
  DeleteTxnTableLockSet(txn, lock_request->lock_mode_, oid);
  delete lock_request;
  if (!q->request_queue_.empty()) {
    g.unlock();
    q->cv_.notify_all();
  }
  // LOG_DEBUG("After notify Quit txn_id: %d, table_oid: %u", txn_id, oid);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (!CanTxnTakeLock(txn, lock_mode)) {
    LOG_DEBUG("txn cannot take lock txn_id: %d table_oid: %u", txn->GetTransactionId(), oid);
    return false;
  }
  auto txn_id = txn->GetTransactionId();
  // intention lock is not allowed for row!
  if (lock_mode == LockMode::INTENTION_SHARED || LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode ||
      lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    LOG_DEBUG("do aborting ATTEMPTED_INTENTION_LOCK_ON_ROW");
    txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  bool is_lock_table = CheckAppropriateLockOnTable(txn, oid, lock_mode);
  if (!is_lock_table) {
    LOG_DEBUG("do aborting TABLE_LOCK_NOT_PRESENT");
    txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  // LOG_DEBUG("is_lock_table ok txn_id: %d", txn->GetTransactionId());
  std::unique_lock<std::mutex> g(row_lock_map_latch_);
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto q = row_lock_map_[rid];  // FIXME: is rid globally unique?
  bool is_compatible = true;
  auto lock_request = GetLockRequest(q.get(), lock_mode, txn_id, is_compatible);
  if (lock_request != nullptr) {
    if (AreCurrentLockSatisfied(lock_request->lock_mode_, lock_mode) && lock_request->granted_) {
      return true;
    }
    // check if there is any txn upgrading
    if (INVALID_TXN_ID != q->upgrading_) {
      LOG_DEBUG("do aborting UPGRADE_CONFLICT upgrading_txn:%d txn_id: %d", q->upgrading_, txn_id);
      txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
    }
    // check can upgrage
    if (lock_request->lock_mode_ != lock_mode && LockMode::EXCLUSIVE != lock_mode) {
      LOG_DEBUG("do aborting INCOMPATIBLE_UPGRADE");
      txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
    }
    q->upgrading_ = txn_id;
    lock_request->granted_ = false;
    DeleteTxnRowLockSet(txn, lock_request->lock_mode_, oid, rid);
    lock_request->lock_mode_ = lock_mode;
  } else {
    lock_request = new LockRequest(txn_id, lock_mode, oid, rid);
    q->request_queue_.push_back(lock_request);
  }
  // we shall wait
  while (!is_compatible) {
    q->cv_.wait(g);
    // check txn status again
    if (!CanTxnTakeLock(txn, lock_mode)) {  // do some cleanup
      LOG_DEBUG("Cleanup Aborted txn txn_id: %d table_oid: %u", txn_id, oid);
      auto iterator = q->request_queue_.begin();
      for (; iterator != q->request_queue_.end() && (*iterator)->txn_id_ != txn_id; ++iterator) {
      }
      if (iterator != q->request_queue_.end()) {
        lock_request = *iterator;
        q->request_queue_.erase(iterator);
      }
      if (txn_id == q->upgrading_) {
        q->upgrading_ = INVALID_TXN_ID;
      }
      delete lock_request;
      q->cv_.notify_all();
      return false;
    }
    // LOG_DEBUG("NOT Compatible keep waiting txn_id: %d table_oid: %u", txn_id, oid);
    is_compatible = true;
    lock_request = GetLockRequest(q.get(), lock_mode, txn_id, is_compatible);
  }
  lock_request->granted_ = true;
  if (txn_id == q->upgrading_) {
    q->upgrading_ = INVALID_TXN_ID;
  }
  UpdateTxnRowLockSet(txn, lock_mode, oid, rid);
  // LOG_DEBUG("Granted row lock txn_id: %d table_oid: %u", txn_id, oid);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // find if we hold the lock
  auto txn_id = txn->GetTransactionId();
  std::unique_lock<std::mutex> g(row_lock_map_latch_);
  auto q = row_lock_map_[rid];
  if (nullptr == q) {
    LOG_DEBUG("do aborting ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD");
    txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto iterator = q->request_queue_.begin();
  bool is_found = false;
  while (iterator != q->request_queue_.end()) {
    if ((*iterator)->txn_id_ == txn_id && (*iterator)->granted_) {
      is_found = true;
      break;
    }
    ++iterator;
  }
  if (!is_found) {  // do aborting ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
    if (!force) {
      txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
      LOG_DEBUG("Aborted: ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD txn_id: %d", txn_id);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
    return true;
  }
  // LOG_DEBUG("check table lock1 txn_id: %d is_found: %d", txn_id, is_found);
  auto lock_request = *iterator;
  bool is_lock_table = CheckAppropriateLockOnTable(txn, oid, lock_request->lock_mode_);
  if (!force && !is_lock_table) {              // do aborting TABLE_LOCK_NOT_PRESENT
    txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
    LOG_DEBUG("Aborted: TABLE_LOCK_NOT_PRESENT txn_id: %d", txn_id);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  if (!force) {
    ChangeTxnState(txn, lock_request->lock_mode_);
  }
  q->request_queue_.erase(iterator);
  DeleteTxnRowLockSet(txn, lock_request->lock_mode_, oid, rid);
  delete lock_request;
  if (!q->request_queue_.empty()) {
    g.unlock();
    q->cv_.notify_all();
  }
  // LOG_DEBUG("After notify Quit txn_id: %d, table_oid: %u", txn_id, oid);
  return true;
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  return false;
}
auto LockManager::UpgradeLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return false;
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::INTENTION_SHARED: {  // IS -> [S, IS, IX, SIX]
      return LockMode::INTENTION_SHARED == l2 || LockMode::INTENTION_EXCLUSIVE == l2 || LockMode::SHARED == l2 ||
             LockMode::SHARED_INTENTION_EXCLUSIVE == l2;
    }
    case LockMode::SHARED: {  // S -> [IS, S]
      return LockMode::SHARED == l2 || LockMode::INTENTION_SHARED == l2;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {  // SIX -> [IS]
      return LockMode::INTENTION_SHARED == l2;
    }
    case LockMode::INTENTION_EXCLUSIVE: {  // IX -> [IS, IX]
      return LockMode::INTENTION_EXCLUSIVE == l2 || LockMode::INTENTION_SHARED == l2;
    }
    default: {  // X -> []
      return false;
    }
  }
  return false;
}

// check txn state
auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  if (nullptr == txn) {
    LOG_DEBUG("txn nullptr!");
    return false;
  }
  auto state = txn->GetState();
  if (TransactionState::ABORTED == state) {
    return false;
  }
  auto iso_level = txn->GetIsolationLevel();
  auto txn_id = txn->GetTransactionId();
  if (IsolationLevel::READ_UNCOMMITTED == iso_level &&
      (lock_mode == LockMode::INTENTION_SHARED || LockMode::SHARED == lock_mode ||
       LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode)) {
    LOG_DEBUG("do aborting LOCK_SHARED_ON_READ_UNCOMMITTED");
    txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
    throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }
  // READ_UNCOMMITTED: X, IX locks are allowed in the GROWING state.
  if (IsolationLevel::READ_UNCOMMITTED == iso_level && state != TransactionState::GROWING &&
      (LockMode::EXCLUSIVE == lock_mode || LockMode::INTENTION_EXCLUSIVE == lock_mode)) {
    LOG_DEBUG("do aborting LOCK_ON_SHRINKING");
    txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
  }
  // READ_COMMITTED: Only IS, S locks are allowed in the SHRINGKING state
  if (IsolationLevel::READ_COMMITTED == iso_level && state == TransactionState::SHRINKING &&
      LockMode::SHARED != lock_mode && LockMode::INTENTION_SHARED != lock_mode) {
    LOG_DEBUG("READ_COMMITTED LOCK_ON_SHRINKING");
    txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
  }
  // REPEATABLE_READ: No locks are allowed in the SHRINKING state
  if (IsolationLevel::REPEATABLE_READ == iso_level && state == TransactionState::SHRINKING) {
    LOG_DEBUG("do aborting LOCK_ON_SHRINKING");
    txn->SetState(TransactionState::ABORTED);  // FIXME: shall we set aborted w/out release locks ?
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
  }
  return true;
}

void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {}

// Note: if curr_lock_mode == requested_lock_mode return false; please do some precheck first
auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  switch (curr_lock_mode) {
    case LockMode::INTENTION_SHARED: {  // IS -> [S, X, IX, SIX]
      return LockMode::INTENTION_EXCLUSIVE == requested_lock_mode || LockMode::SHARED == requested_lock_mode ||
             LockMode::SHARED_INTENTION_EXCLUSIVE == requested_lock_mode || LockMode::EXCLUSIVE == requested_lock_mode;
    }
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE: {  // IX -> [X, SIX] || S -> [X, SIX]
      return LockMode::EXCLUSIVE == requested_lock_mode || LockMode::SHARED_INTENTION_EXCLUSIVE == requested_lock_mode;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {  // SIX -> [X]
      return LockMode::EXCLUSIVE == requested_lock_mode;
    }
    default: {
      return false;
    }
  }
  return false;
}

// if current lock satisfied, no need to regain lock
auto LockManager::AreCurrentLockSatisfied(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  switch (curr_lock_mode) {
    case LockMode::INTENTION_SHARED: {  // IS -> [IS]
      return LockMode::INTENTION_SHARED == requested_lock_mode;
    }
    case LockMode::SHARED: {  // S -> [IS, S]
      return LockMode::INTENTION_SHARED == requested_lock_mode || LockMode::SHARED == requested_lock_mode;
    }
    case LockMode::INTENTION_EXCLUSIVE: {  // IX -> [IS, IX]
      return LockMode::INTENTION_EXCLUSIVE == requested_lock_mode || LockMode::INTENTION_SHARED == requested_lock_mode;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {  // SIX -> [IX, SIX, S, IS]
      return LockMode::INTENTION_SHARED == requested_lock_mode || LockMode::SHARED == requested_lock_mode ||
             LockMode::SHARED_INTENTION_EXCLUSIVE == requested_lock_mode ||
             LockMode::INTENTION_SHARED == requested_lock_mode;
    }
    case LockMode::EXCLUSIVE: {  // X -> [X, IX, SIX, S, IS]
      return LockMode::INTENTION_SHARED == requested_lock_mode || LockMode::SHARED == requested_lock_mode ||
             LockMode::SHARED_INTENTION_EXCLUSIVE == requested_lock_mode ||
             LockMode::EXCLUSIVE == requested_lock_mode || LockMode::INTENTION_SHARED == requested_lock_mode;
    }
    default: {
      return false;
    }
  }
  return false;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  std::unique_lock<std::mutex> g(table_lock_map_latch_);
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    return false;
  }
  auto &q = table_lock_map_[oid];
  auto txn_id = txn->GetTransactionId();
  auto iterator = q->request_queue_.begin();
  while (iterator != q->request_queue_.end()) {
    if ((*iterator)->txn_id_ == txn_id && (*iterator)->granted_) {
      break;
    }
    ++iterator;
  }
  if (iterator == q->request_queue_.end()) {
    LOG_DEBUG("CheckAppropriateLockOnTable Not Found txn_id: %d", txn_id);
    return false;
  }
  auto cur_lock_mode = (*iterator)->lock_mode_;
  if (row_lock_mode == LockMode::EXCLUSIVE) {  // X -> X IX SIX
    return LockMode::INTENTION_EXCLUSIVE == cur_lock_mode || LockMode::EXCLUSIVE == cur_lock_mode ||
           LockMode::SHARED_INTENTION_EXCLUSIVE == cur_lock_mode;
  }
  // S -> S IS X IX SIX
  return row_lock_mode == LockMode::SHARED;
}

// change txn state according to iso_level
void LockManager::ChangeTxnState(Transaction *txn, LockMode lock_mode) {
  auto state = txn->GetState();
  if (TransactionState::GROWING != state) {
    return;
  }
  if (LockMode::EXCLUSIVE == lock_mode && state != TransactionState::SHRINKING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  if (IsolationLevel::REPEATABLE_READ == txn->GetIsolationLevel() && LockMode::SHARED == lock_mode) {
    txn->SetState(TransactionState::SHRINKING);
  }
}

auto LockManager::GetLockRequest(LockRequestQueue *lock_request_queue, LockMode lock_mode, txn_id_t txn_id,
                                 bool &is_compatible) -> LockRequest * {
  if (nullptr == lock_request_queue) {
    return nullptr;
  }
  LockRequest *lock_request = nullptr;
  for (auto lq : lock_request_queue->request_queue_) {
    if (lq->txn_id_ == txn_id) {
      lock_request = lq;
      continue;
    }
    if (!AreLocksCompatible(lq->lock_mode_, lock_mode) && (lock_request == nullptr || lq->granted_)) {
      is_compatible = false;
    }
    if (!is_compatible && lock_request != nullptr) {
      break;
    }
  }
  return lock_request;
}

auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
  return false;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
  // Cleanup all locks
  std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
  for (auto &table_lock_pair : table_lock_map_) {
    auto q = table_lock_pair.second;
    if (nullptr == q) {
      continue;
    }
    for (auto iter : q->request_queue_) {
      delete iter;
    }
    q->request_queue_.clear();
  }
  table_lock.unlock();
  std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
  for (auto &row_lock_pair : row_lock_map_) {
    auto q = row_lock_pair.second;
    if (nullptr == q) {
      continue;
    }
    for (auto iter : q->request_queue_) {
      delete iter;
    }
    q->request_queue_.clear();
  }
}

// Update txn set after grant table lock successfully.
void LockManager::UpdateTxnTableLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED: {
      auto is_table_lock_set = txn->GetIntentionSharedTableLockSet();
      is_table_lock_set->emplace(oid);
      return;
    }
    case LockMode::SHARED: {
      auto s_table_lock_set = txn->GetSharedTableLockSet();
      s_table_lock_set->emplace(oid);
      return;
    }
    case LockMode::INTENTION_EXCLUSIVE: {
      auto ix_table_lock_set = txn->GetIntentionExclusiveTableLockSet();
      ix_table_lock_set->emplace(oid);
      return;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      auto six_table_lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
      six_table_lock_set->emplace(oid);
      return;
    }
    case LockMode::EXCLUSIVE: {
      auto x_table_lock_set = txn->GetExclusiveTableLockSet();
      x_table_lock_set->emplace(oid);
      return;
    }
    default: {
      return;
    }
  }
}

// Update txn set after grant row lock successfully.
void LockManager::UpdateTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  switch (lock_mode) {
    case LockMode::SHARED: {
      auto s_row_lock_set = txn->GetSharedRowLockSet();
      (*s_row_lock_set)[oid].emplace(rid);
      return;
    }
    case LockMode::EXCLUSIVE: {
      auto x_row_lock_set = txn->GetExclusiveRowLockSet();
      (*x_row_lock_set)[oid].emplace(rid);
      return;
    }
    default: {
      return;
    }
  }
}

void LockManager::DeleteTxnTableLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED: {
      auto is_table_lock_set = txn->GetIntentionSharedTableLockSet();
      is_table_lock_set->erase(oid);
      return;
    }
    case LockMode::SHARED: {
      auto s_table_lock_set = txn->GetSharedTableLockSet();
      s_table_lock_set->erase(oid);
      return;
    }
    case LockMode::INTENTION_EXCLUSIVE: {
      auto ix_table_lock_set = txn->GetIntentionExclusiveTableLockSet();
      ix_table_lock_set->erase(oid);
      return;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      auto six_table_lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
      six_table_lock_set->erase(oid);
      return;
    }
    case LockMode::EXCLUSIVE: {
      auto x_table_lock_set = txn->GetExclusiveTableLockSet();
      x_table_lock_set->erase(oid);
      return;
    }
    default: {
      return;
    }
  }
}

void LockManager::DeleteTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  switch (lock_mode) {
    case LockMode::SHARED: {
      auto s_row_lock_set = txn->GetSharedRowLockSet();
      (*s_row_lock_set)[oid].erase(rid);
      return;
    }
    case LockMode::EXCLUSIVE: {
      auto x_row_lock_set = txn->GetExclusiveRowLockSet();
      (*x_row_lock_set)[oid].erase(rid);
      return;
    }
    default: {
      return;
    }
  }
}
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::lock_guard<std::mutex> g(waits_for_latch_);
  auto &v = waits_for_[t1];
  // auto txn = txn_manager_->GetTransaction(t2);
  // auto iterator = std::find(v.begin(), v.end(), t2);
  v.insert(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::lock_guard<std::mutex> g(waits_for_latch_);
  auto &v = waits_for_[t1];
  auto iterator = std::find(v.begin(), v.end(), t2);
  if (iterator != v.end()) {
    v.erase(iterator);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::lock_guard<std::mutex> g(waits_for_latch_);
  // DFS
  std::set<txn_id_t> d;
  std::set<txn_id_t> visited;
  if (waits_for_.empty()) {
    LOG_DEBUG("Waits_for empty");
    return false;
  }
  for (const auto &wait_for_pair : waits_for_) {
    d.insert(wait_for_pair.first);
  }
  while (!d.empty()) {
    auto cur_txn_id = *d.begin();
    d.erase(d.begin());
    for (auto waits_for_txn_id : waits_for_[cur_txn_id]) {
      if (visited.find(waits_for_txn_id) == visited.end()) {
        LOG_DEBUG("cur_txn_id(%d) waits for waits_for_txn_id(%d)", cur_txn_id, waits_for_txn_id);
        d.insert(waits_for_txn_id);
      } else {  // waits_for_ is sorted, the latter one must be the youngest transaction!
        LOG_DEBUG("cur_txn_id(%d) waits for waits_for_txn_id(%d) which is visited", cur_txn_id, waits_for_txn_id);
        *txn_id = cur_txn_id;
        return true;
      }
    }
    visited.insert(cur_txn_id);
  }
  LOG_DEBUG("No cycle is found size: %zu", waits_for_.size());
  return false;
}

void LockManager::RemoveWaitsForKey(txn_id_t txn_id) {
  std::lock_guard<std::mutex> g(waits_for_latch_);
  waits_for_.erase(txn_id);
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::lock_guard<std::mutex> g(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (const auto &waits_for_pair : waits_for_) {
    for (const auto &txn_id : waits_for_pair.second) {
      edges.emplace_back(waits_for_pair.first, txn_id);
    }
  }
  return edges;
}

void LockManager::BuildGraph() {
  std::lock_guard<std::mutex> g(waits_for_latch_);
  // LOG_DEBUG("BuildGraph from empty");
  waits_for_.clear();
  // build waits for graph by traversing lock_request_queue
  std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
  for (const auto &table_lock_pair : table_lock_map_) {
    const auto &q = table_lock_pair.second;
    if (nullptr != q) {
      std::set<txn_id_t> waits_for;
      for (const auto &iter : q->request_queue_) {
        auto txn = txn_manager_->GetTransaction(iter->txn_id_);
        if (nullptr == txn) {
          continue;
        }
        if (TransactionState::ABORTED == txn->GetState()) {  // wakeup and do some cleanup
          LOG_DEBUG("Aborted txn_id: %d wakeup", iter->txn_id_);
          q->cv_.notify_all();
          continue;
        }
        if (!iter->granted_) {
          waits_for_[iter->txn_id_] = waits_for;
        }
        waits_for.insert(iter->txn_id_);
      }
    }
  }
  table_lock.unlock();
  std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
  for (const auto &row_lock_pair : row_lock_map_) {
    const auto &q = row_lock_pair.second;
    if (nullptr != q) {
      std::set<txn_id_t> waits_for;
      for (const auto &iter : q->request_queue_) {
        auto txn = txn_manager_->GetTransaction(iter->txn_id_);
        if (nullptr == txn) {
          continue;
        }
        if (TransactionState::ABORTED == txn->GetState()) {  // wakeup and do some cleanup
          LOG_DEBUG("Aborted txn_id: %d wakeup", iter->txn_id_);
          q->cv_.notify_all();
          continue;
        }
        if (!iter->granted_) {
          for (const auto &waits_for_txn_id : waits_for) {
            waits_for_[iter->txn_id_].insert(waits_for_txn_id);
          }
        }
        waits_for.insert(iter->txn_id_);
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      txn_id_t abort_txn_id = INVALID_TXN_ID;
      // make sure there is no cycle remained!
      BuildGraph();
      while (HasCycle(&abort_txn_id)) {
        // LOG_DEBUG("HasCycle txn_id: %d!", abort_txn_id);
        auto txn = txn_manager_->GetTransaction(abort_txn_id);
        if (nullptr != txn) {
          // LOG_DEBUG("HasCycle abort_xn_id: %d!", abort_txn_id);
          txn->SetState(TransactionState::ABORTED);
          RemoveWaitsForKey(abort_txn_id);
        }
      }
    }
  }
}

}  // namespace bustub
