//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  RevertWriteSet(txn);

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::RevertWriteSet(Transaction *txn) {
  txn->LockTxn();
  auto table_write_set = txn->GetWriteSet();
  auto index_write_set = txn->GetIndexWriteSet();
  while (!table_write_set->empty()) {
    auto record = table_write_set->back();
    table_write_set->pop_back();
    if (WType::INSERT == record.wtype_) {
      record.table_heap_->UpdateTupleMeta(TupleMeta{txn->GetTransactionId(), txn->GetTransactionId(), true},
                                          record.rid_);
    } else if (WType::DELETE == record.wtype_) {
      record.table_heap_->UpdateTupleMeta(TupleMeta{txn->GetTransactionId(), txn->GetTransactionId(), false},
                                          record.rid_);
    }
  }
  while (!index_write_set->empty()) {
    auto record = index_write_set->back();
    index_write_set->pop_back();
    auto index_info = record.catalog_->GetIndex(record.index_oid_);
    auto table_info = record.catalog_->GetTable(record.table_oid_);
    if (WType::INSERT == record.wtype_) {
      index_info->index_->DeleteEntry(
          record.tuple_.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          record.rid_, txn);
    } else if (WType::DELETE == record.wtype_) {
      index_info->index_->InsertEntry(
          record.tuple_.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          record.rid_, txn);
    }
  }
  txn->UnlockTxn();
}
void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
