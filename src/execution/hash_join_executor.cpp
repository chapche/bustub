//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
  }
}

void HashJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  right_ht_.Clear();
  Tuple right_tuple;
  RID right_rid;
  const auto right_schema = plan_->GetRightPlan()->OutputSchema();
  int right_col_cnt = right_schema.GetColumnCount();
  while (true) {
    bool res = right_child_executor_->Next(&right_tuple, &right_rid);
    if (!res) {
      break;
    }
    const auto &join_key = GetJoinKey(&right_tuple, right_schema, plan_->RightJoinKeyExpressions());
    JoinValue join_value;
    for (int i = 0; i < right_col_cnt; i++) {
      join_value.values_.push_back(right_tuple.GetValue(&right_schema, i));
    }
    right_ht_.Insert(join_key, join_value);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> values;
  bool res = true;
  const auto &left_schema = plan_->GetLeftPlan()->OutputSchema();
  const auto &right_schema = plan_->GetRightPlan()->OutputSchema();
  JoinValue join_value;
  while (true) {
    if (start_from_new_one_) {
      res = left_child_executor_->Next(&last_left_tuple_, rid);  // filter push down
      if (!res) {
        return false;
      }
    }
    const auto &left_join_key = GetJoinKey(&last_left_tuple_, left_schema, plan_->LeftJoinKeyExpressions());
    if (start_from_new_one_) {
      // make sure we search from the start if we start from a new one
      last_index_ = 0;
      has_matched_ = false;
    }
    const auto &join_values = right_ht_.GetValue(left_join_key);
    // LOG_DEBUG("last_left_tuple: %s", last_left_tuple_.ToString(&left_schema).c_str());
    // LOG_DEBUG("size: %zu last index: %d", join_values.size(), last_index_);
    if (!join_values.empty() && static_cast<int>(join_values.size()) > last_index_) {
      start_from_new_one_ = false;
      has_matched_ = true;
      join_value = join_values[last_index_++];
      break;
    }
    if (JoinType::LEFT == plan_->GetJoinType()) {
      if (has_matched_) {
        has_matched_ = false;
      } else {
        res = false;
        break;
      }
    }
    start_from_new_one_ = true;
  }
  int left_col_cnt = left_schema.GetColumnCount();
  int right_col_cnt = right_schema.GetColumnCount();
  values.reserve(left_col_cnt + right_col_cnt);
  for (int i = 0; i < left_col_cnt; i++) {
    values.push_back(last_left_tuple_.GetValue(&left_schema, i));
  }
  if (!res) {
    for (int i = 0; i < right_col_cnt; i++) {
      auto col = right_schema.GetColumn(i);
      values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
    }
  } else {
    for (const auto &value : join_value.values_) {
      values.push_back(value);
    }
  }
  auto output_schema = NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan(), *plan_->GetRightPlan());
  *tuple = Tuple{values, &output_schema};
  return true;
}

}  // namespace bustub
