//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_excuted_) {
    return false;
  }
  int cnt = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto index_info_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  while (true) {
    bool res = child_executor_->Next(tuple, rid);
    if (!res) {
      break;
    }
    cnt++;
    table_info->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, *rid);
    for (auto index_info : index_info_vec) {
      index_info->index_->DeleteEntry(
          tuple->KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
          nullptr);
    }
  }
  std::vector<Value> values{Value(TypeId::INTEGER, cnt)};
  *tuple = Tuple{values, &GetOutputSchema()};
  is_excuted_ = true;
  return true;
}

}  // namespace bustub
