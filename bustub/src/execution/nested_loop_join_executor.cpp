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

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  Tuple ltuple;
  Tuple rtuple;
  RID lrid;
  RID rrid;
  left_executor_->Init();
  try {
    while (left_executor_->Next(&ltuple, &lrid)) {
      right_executor_->Init();
      while (right_executor_->Next(&rtuple, &rrid)) {
        if (plan_->Predicate() == nullptr ||
            plan_->Predicate()
                ->EvaluateJoin(&ltuple, left_executor_->GetOutputSchema(), &rtuple, right_executor_->GetOutputSchema())
                .GetAs<bool>()) {
          std::vector<Value> vals;
          for (const auto &col : plan_->OutputSchema()->GetColumns()) {
            vals.emplace_back(col.GetExpr()->EvaluateJoin(&ltuple, left_executor_->GetOutputSchema(), &rtuple,
                                                          right_executor_->GetOutputSchema()));
          }
          ret_.emplace_back(Tuple(vals, GetOutputSchema()));
          break;
        }
      }
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }
  it_ = ret_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == ret_.end()) {
    return false;
  }
  *tuple = *it_;
  *rid = tuple->GetRid();
  ++it_;
  return true;
}

}  // namespace bustub
