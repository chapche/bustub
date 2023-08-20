//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_visited_) {
    return false;
  }
  // ValuesExcutor
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  if (nullptr == table_info) {
    return false;
  }
  bool res = false;
  int cnt = 0;
  while (true) {
    res = child_executor_->Next(tuple, rid);
    if (!res) {
      break;
    }
    cnt++;
    auto r = table_info->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, *tuple);
    // update indexes
    auto index_info_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (auto index_info : index_info_vec) {
      index_info->index_->InsertEntry(
          tuple->KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          r.value(), nullptr);
    }
  }
  std::vector<Value> values{Value(TypeId::INTEGER, cnt)};
  *tuple = Tuple{values, &GetOutputSchema()};
  is_visited_ = true;
  return true;
}

}  // namespace bustub
