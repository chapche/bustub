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
  itor_ = std::make_shared<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // get table iterators
  // search for value
  if (nullptr == itor_ || itor_->IsEnd()) {
    // EXECUTOR EXHAUSTED
    return false;
  }
  // check is_delete
  for (; !itor_->IsEnd(); ++(*itor_)) {
    auto tuple_meta = itor_->GetTuple().first;
    if (!tuple_meta.is_deleted_) {
      *tuple = itor_->GetTuple().second;
      *rid = itor_->GetRID();
      ++(*itor_);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
