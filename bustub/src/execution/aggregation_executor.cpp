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
      aht_(SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {
  plan_ = plan;
  child_ = std::move(child);
}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tmptuple;
  RID tmprid;
  try {
    while (child_->Next(&tmptuple, &tmprid)) {
      aht_.InsertCombine(MakeAggregateKey(&tmptuple), MakeAggregateValue(&tmptuple));
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  const AggregateKey &key = aht_iterator_.Key();
  const AggregateValue &val = aht_iterator_.Val();
  ++aht_iterator_;
  if (plan_->GetHaving() == nullptr ||
      plan_->GetHaving()->EvaluateAggregate(key.group_bys_, val.aggregates_).GetAs<bool>()) {
    std::vector<Value> ret;
    for (const auto &col : plan_->OutputSchema()->GetColumns()) {
      ret.emplace_back(col.GetExpr()->EvaluateAggregate(key.group_bys_, val.aggregates_));
    }
    *tuple = Tuple(ret, plan_->OutputSchema());
    *rid = tuple->GetRid();
    return true;
  }
  return Next(tuple, rid);
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
