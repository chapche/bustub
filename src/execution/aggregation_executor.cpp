//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  aht_.Clear();
  while (true) {
    bool res = child_->Next(&tuple, &rid);
    if (!res) {
      break;
    }
    // group by first
    AggregateKey agg_key;
    AggregateValue agg_value;
    for (const auto &exp : plan_->GetGroupBys()) {
      agg_key.group_bys_.push_back(exp->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    for (const auto &exp : plan_->GetAggregates()) {
      agg_value.aggregates_.push_back(exp->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    // agg the value
    aht_.InsertCombine(agg_key, agg_value);
  }
  if (aht_.Begin() == aht_.End()) {
    aht_.Init();
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;
  int group_by_size = plan_->GetGroupBys().size();
  const auto &keys = aht_iterator_.Key().group_bys_;
  int key_size = keys.size();
  if (key_size != group_by_size) {  // invalid tuple
    ++aht_iterator_;
    return false;
  }
  const auto &vals = aht_iterator_.Val().aggregates_;
  int val_size = vals.size();
  values.reserve(key_size + val_size);
  for (int i = 0; i < key_size; i++) {
    values.push_back(keys[i]);
  }
  for (int i = 0; i < val_size; i++) {
    values.push_back(vals[i]);
  }
  auto output_schema =
      AggregationPlanNode::InferAggSchema(plan_->GetGroupBys(), plan_->GetAggregates(), plan_->GetAggregateTypes());
  *tuple = Tuple{values, &output_schema};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
