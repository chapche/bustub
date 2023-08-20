//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  itor_ = std::make_shared<BPlusTreeIndexIteratorForTwoIntegerColumn>(tree->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!itor_ || itor_->IsEnd()) {
    return false;
  }
  for (; !itor_->IsEnd(); ++(*itor_)) {
    auto rid_value = (*(*itor_)).second;
    auto pair = table_info_->table_->GetTuple(rid_value);
    if (!pair.first.is_deleted_) {
      *tuple = pair.second;
      *rid = rid_value;
      ++(*itor_);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
