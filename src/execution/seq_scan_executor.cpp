//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto exec_ctx = GetExecutorContext();
  if (nullptr == exec_ctx) {
    return;
  }
  auto table_info = exec_ctx->GetCatalog()->GetTable(plan_->table_name_);
  itor_ = std::make_shared<TableIterator>(table_info->table_->MakeEagerIterator());
  auto txn = exec_ctx->GetTransaction();
  if (exec_ctx_->IsDelete()) {  // Get X locks for table
    bool res =
        txn->IsTableIntentionExclusiveLocked(table_info->oid_) ||
        exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info->oid_);
    if (!res) {
      LOG_DEBUG("SeqScan GetTableLock Failed!");
      throw ExecutionException("SeqScan GetTableLock Failed!");
    }
  } else if (IsolationLevel::REPEATABLE_READ == txn->GetIsolationLevel() ||
             IsolationLevel::READ_COMMITTED == txn->GetIsolationLevel()) {
    bool res = txn->IsTableIntentionExclusiveLocked(table_info->oid_) ||
               exec_ctx->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, table_info->oid_);
    if (!res) {
      LOG_DEBUG("SeqScan GetTableLock Failed!");
      throw ExecutionException("SeqScan GetTableLock Failed!");
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // get table iterators
  // search for value
  if (nullptr == itor_ || itor_->IsEnd()) {
    // EXECUTOR EXHAUSTED
    return false;
  }
  // check is_delete
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_);
  auto txn = exec_ctx_->GetTransaction();
  for (; !itor_->IsEnd(); ++(*itor_)) {
    *rid = itor_->GetRID();
    if (exec_ctx_->IsDelete()) {
      bool res = txn->IsRowExclusiveLocked(table_info->oid_, *rid) ||
                 exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info->oid_, *rid);
      if (!res) {
        LOG_DEBUG("SeqScan GetRowLock Failed!");
        throw ExecutionException("SeqScan GetRowLock Failed!");
      }
    } else if (IsolationLevel::REPEATABLE_READ == txn->GetIsolationLevel() ||
               IsolationLevel::READ_COMMITTED == txn->GetIsolationLevel()) {
      bool res = txn->IsRowExclusiveLocked(table_info->oid_, *rid) ||
                 exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, table_info->oid_, *rid);
      if (!res) {
        LOG_DEBUG("SeqScan GetRowLock Failed!");
        throw ExecutionException("SeqScan GetRowLock Failed!");
      }
    }
    auto tuple_meta = itor_->GetTuple().first;
    if (!tuple_meta.is_deleted_) {
      *tuple = itor_->GetTuple().second;
      if (IsolationLevel::READ_COMMITTED == txn->GetIsolationLevel() &&
          txn->IsRowSharedLocked(table_info->oid_, *rid)) {
        exec_ctx_->GetLockManager()->UnlockRow(txn, table_info->oid_, *rid, true);
        // LOG_DEBUG("SeqScan UnlockRowLock1");
      }
      ++(*itor_);
      return true;
    }
    if (exec_ctx_->IsDelete() || (IsolationLevel::REPEATABLE_READ == txn->GetIsolationLevel() ||
                                  IsolationLevel::READ_COMMITTED == txn->GetIsolationLevel())) {
      exec_ctx_->GetLockManager()->UnlockRow(txn, table_info->oid_, *rid, true);
      // LOG_DEBUG("SeqScan UnlockRowLock2");
    }
  }
  return false;
}

}  // namespace bustub
