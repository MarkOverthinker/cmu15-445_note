//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), it_(TableIterator(nullptr, RID(), nullptr)) {
  plan_ = plan;
}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ = table_info_->table_.get();
  it_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == table_heap_->End()) {
    return false;
  }
  LockManager *lckm = exec_ctx_->GetLockManager();
  Transaction *txn = exec_ctx_->GetTransaction();
  RID tmprid = it_->GetRid();
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED && lckm != nullptr) {
    lckm->LockShared(txn, tmprid);
  }
  const Schema *output_schema = plan_->OutputSchema();
  const Schema *table_schema = &(table_info_->schema_);
  std::vector<Value> vals;
  size_t cnt = output_schema->GetColumnCount();
  for (size_t i = 0; i < cnt; ++i) {
    vals.emplace_back(output_schema->GetColumn(i).GetExpr()->Evaluate(&(*it_), table_schema));
  }
  Tuple tmptuple(vals, output_schema);
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lckm != nullptr) {
    lckm->Unlock(txn, tmprid);
  }
  ++it_;
  if (plan_->GetPredicate() == nullptr || plan_->GetPredicate()->Evaluate(&tmptuple, output_schema).GetAs<bool>()) {
    *tuple = tmptuple;
    *rid = tmprid;
    return true;
  }
  return Next(tuple, rid);
}

}  // namespace bustub
