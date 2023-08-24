#include <algorithm>

#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  index_ = 0;
  vals_.clear();
  while (true) {
    Tuple tuple;
    bool res = child_executor_->Next(&tuple, &rid);
    if (!res) {
      break;
    }
    vals_.emplace_back(tuple);
  }
  auto comparator = [&](const Tuple &a, const Tuple &b) -> bool {
    for (const auto &p : plan_->GetOrderBy()) {
      auto left_value = p.second->Evaluate(&a, child_executor_->GetOutputSchema());
      auto right_value = p.second->Evaluate(&b, child_executor_->GetOutputSchema());
      auto order_by = p.first;
      if ((OrderByType::ASC == order_by || OrderByType::DEFAULT == order_by)) {
        if (left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) {
          return true;
        }
        if (left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue) {
          return false;
        }
      }
      if (OrderByType::DESC == order_by) {
        if (left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue) {
          return true;
        }
        if (left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) {
          return false;
        }
      }
    }
    return false;
  };
  std::sort(vals_.begin(), vals_.end(), comparator);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (vals_.size() <= index_) {
    return false;
  }
  *tuple = vals_[index_++];
  // *rid = ;
  return true;
}

}  // namespace bustub
