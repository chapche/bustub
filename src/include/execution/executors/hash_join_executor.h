//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/** JoinKey represents a key in a join operation */
struct JoinKey {
  std::vector<Value> keys_;

  /**
   * Compares two Join keys for equality.
   * @param other the other Join key to be compared with
   * @return `true` if both Join keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const JoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.keys_.size(); i++) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** JoinValue represents a value for each of the running Joins */
struct JoinValue {
  /** The Join values */
  std::vector<Value> values_;
};
}  // namespace bustub

namespace std {

/** Implements std::hash on JoinKey */
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {
/**
 * A simplified hash table that has all the necessary functionality for join.
 */
class SimpleJoinHashTable {
 public:
  /**
   * Construct a new SimpleJoinHashTable instance.
   * @param key_exprs the join key expressions
   */
  SimpleJoinHashTable() = default;

  /**
   * Inserts a value into the hash table and then combines it with the current Join. Must handle collisions!
   * @param join_key the key to be inserted
   * @param join_val the value to be inserted
   */
  void Insert(const JoinKey &join_key, const JoinValue &join_val) {
    if (ht_.count(join_key) == 0) {
      std::vector<JoinValue> values;
      values.push_back(join_val);
      ht_.insert({join_key, values});
    } else {
      auto &values = ht_[join_key];
      values.push_back(join_val);
    }
  }

  auto GetValue(const JoinKey &join_key) const -> std::vector<JoinValue> {
    if (ht_.find(join_key) != ht_.end()) {
      return ht_.at(join_key);
    }
    return {};
  }
  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

 private:
  /** The hash table is just a map from join keys to values */
  std::unordered_map<JoinKey, std::vector<JoinValue>> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  /** @return The join key */
  auto GetJoinKey(const Tuple *tuple, const Schema &schema,
                  const std::vector<AbstractExpressionRef> &key_expressions) const -> JoinKey {
    JoinKey join_key;
    for (const auto &key_exp : key_expressions) {
      join_key.keys_.push_back(key_exp->Evaluate(tuple, schema));
    }
    return join_key;
  }

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_executor_{nullptr};
  std::unique_ptr<AbstractExecutor> right_child_executor_{nullptr};
  SimpleJoinHashTable right_ht_;
  int last_index_{0};
  bool start_from_new_one_{true};
  bool has_matched_{false};
  Tuple last_left_tuple_;
};

}  // namespace bustub
