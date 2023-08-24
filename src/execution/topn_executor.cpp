#include <algorithm>

#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), heap_size_(plan->GetN()) {
  heap_.reserve(heap_size_);
}

void TopNExecutor::Init() {
  cnt_ = 0;
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  bool is_heap = false;
  heap_.clear();
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
  // init heap
  while (true) {
    bool res = child_executor_->Next(&tuple, &rid);
    if (!res) {
      break;
    }
    // adjust heap
    if (heap_.size() < heap_size_) {
      heap_.push_back(tuple);
    } else {
      if (heap_.size() == heap_size_) {
        std::make_heap(heap_.begin(), heap_.end(), comparator);
        is_heap = true;
      }
      if (comparator(tuple, heap_[0])) {
        std::pop_heap(heap_.begin(), heap_.end(), comparator);
        heap_[heap_size_ - 1] = tuple;
        std::push_heap(heap_.begin(), heap_.end(), comparator);
      }
    }
  }
  if (!is_heap) {
    std::make_heap(heap_.begin(), heap_.end(), comparator);
  }
  std::sort_heap(heap_.begin(), heap_.end(), comparator);
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cnt_ >= heap_.size()) {
    return false;
  }
  *tuple = heap_[cnt_++];
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_.size(); }

}  // namespace bustub
