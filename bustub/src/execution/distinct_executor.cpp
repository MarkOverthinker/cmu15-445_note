//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple tmptuple;
  RID tmprid;
  try {
    while (child_executor_->Next(&tmptuple, &tmprid)) {
      DistinctKey key;
      size_t n = plan_->OutputSchema()->GetColumnCount();
      for (size_t i = 0; i < n; ++i) {
        key.colvals_.emplace_back(tmptuple.GetValue(plan_->OutputSchema(), i));
      }
      ht_.insert({key, tmptuple});
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
  }
  it_ = ht_.begin();
}

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == ht_.end()) {
    return false;
  }
  *tuple = it_->second;
  *rid = tuple->GetRid();
  it_++;
  return true;
}

}  // namespace bustub
