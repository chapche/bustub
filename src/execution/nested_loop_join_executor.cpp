//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_executor)),
      right_child_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
  }
}

void NestedLoopJoinExecutor::Init() { left_child_executor_->Init(); }

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> values;
  auto join_filter_expr = plan_->Predicate();
  bool res = true;
  Tuple right_tuple;
  RID right_rid;
  const auto &left_schema = plan_->GetLeftPlan()->OutputSchema();
  const auto &right_schema = plan_->GetRightPlan()->OutputSchema();
  while (true) {
    if (start_from_new_one_) {
      res = left_child_executor_->Next(&last_left_tuple_, rid);  // filter push down
      if (!res) {
        return false;
      }
    }
    // do some cleanup
    right_tuple = Tuple();
    if (start_from_new_one_) {
      // make sure we search from the start if we start from a new one
      right_child_executor_->Init();
    }
    Value value;
    while (true) {
      res = right_child_executor_->Next(&right_tuple, &right_rid);
      if (!res) {
        start_from_new_one_ = true;
        break;
      }
      // predicate expression always return boolean according to filter expression
      value = join_filter_expr->EvaluateJoin(&last_left_tuple_, left_schema, &right_tuple, right_schema);
      if (!value.IsNull() && value.GetAs<bool>()) {  // we have matched value!
        start_from_new_one_ = false;
        break;
      }
    }
    if ((!value.IsNull() && value.GetAs<bool>())) {
      has_matched_ = true;
      break;
    }
    if ((!res && JoinType::LEFT == plan_->GetJoinType())) {
      if (has_matched_) {
        start_from_new_one_ = true;
        has_matched_ = false;
      } else {
        break;
      }
    }
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
    for (int i = 0; i < right_col_cnt; i++) {
      values.push_back(right_tuple.GetValue(&right_schema, i));
    }
  }
  auto output_schema = NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan(), *plan_->GetRightPlan());
  *tuple = Tuple{values, &output_schema};
  return true;
}

}  // namespace bustub
