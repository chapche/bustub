//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  if (nullptr != exec_ctx_) {
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  }
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // scan and find the tuple
  if (is_visited_) {
    return false;
  }
  int cnt = 0;
  auto index_info_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  while (true) {
    bool res = child_executor_->Next(tuple, rid);
    if (!res) {
      break;
    }
    cnt++;
    std::vector<Value> values;
    for (const auto &exp : plan_->target_expressions_) {
      if (!exp) {
        continue;
      }
      values.push_back(exp->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    table_info_->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, *rid);
    for (auto index_info : index_info_vec) {
      index_info->index_->DeleteEntry(
          tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
          nullptr);
    }
    auto new_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
    auto r = table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple);
    if (!r.has_value()) {
      return false;
    }
    for (auto index_info : index_info_vec) {
      index_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          r.value(), nullptr);
    }
  }
  is_visited_ = true;
  std::vector<Value> values{Value(TypeId::INTEGER, cnt)};
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
