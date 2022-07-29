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

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_child_executor_ = std::move(left_child);
  right_child_executor_ = std::move(right_child);
  it_ = ret_.begin();
}

void HashJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  map_.clear();
  try {
    Tuple tmptuple;
    RID tmprid;
    while (left_child_executor_->Next(&tmptuple, &tmprid)) {
      HashJoinKey key;
      key.column_value_ = plan_->LeftJoinKeyExpression()->Evaluate(&tmptuple, left_child_executor_->GetOutputSchema());
      if (map_.find(key) != map_.end()) {
        map_[key].emplace_back(tmptuple);
      } else {
        map_[key] = std::vector{tmptuple};
      }
    }
    while (right_child_executor_->Next(&tmptuple, &tmprid)) {
      HashJoinKey key;
      key.column_value_ =
          plan_->RightJoinKeyExpression()->Evaluate(&tmptuple, right_child_executor_->GetOutputSchema());
      if (map_.find(key) != map_.end()) {
        for (const auto &ltuple : map_[key]) {
          std::vector<Value> vals;
          for (const auto &col : plan_->OutputSchema()->GetColumns()) {
            vals.emplace_back(col.GetExpr()->EvaluateJoin(&ltuple, left_child_executor_->GetOutputSchema(), &tmptuple,
                                                          right_child_executor_->GetOutputSchema()));
          }
          ret_.emplace_back(Tuple(vals, GetOutputSchema()));
        }
      }
    }
    it_ = ret_.begin();
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == ret_.end()) {
    return false;
  }
  *tuple = *it_;
  *rid = tuple->GetRid();
  ++it_;
  return true;
}

}  // namespace bustub
