//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::InsertTupleIndex(Tuple isr_tuple, RID *rid) {
  if (!table_heap_->InsertTuple(isr_tuple, rid, exec_ctx_->GetTransaction())) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "failed to insert tuple");
  }
  Transaction *txn = exec_ctx_->GetTransaction();
  LockManager *lckm = exec_ctx_->GetLockManager();
  if (lckm != nullptr) {
    if (txn->IsSharedLocked(*rid)) {
      lckm->LockUpgrade(txn, *rid);
    } else {
      lckm->LockExclusive(txn, *rid);
    }
  }
  for (auto &index : indexes_) {
    index->index_->InsertEntry(
        isr_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
        *rid, exec_ctx_->GetTransaction());
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lckm != nullptr) {
    lckm->Unlock(txn, *rid);
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (plan_->IsRawInsert()) {
    auto values = plan_->RawValues();
    const Schema insert_schema = table_info_->schema_;
    for (const auto &val : values) {
      InsertTupleIndex(Tuple(val, &insert_schema), rid);
    }
  } else {
    child_executor_->Init();
    try {
      while (child_executor_->Next(tuple, rid)) {
        InsertTupleIndex(*tuple, rid);
      }
    } catch (Exception &e) {
      throw Exception(e.GetType(), "insert tuple in child_execution_insert faild");
    }
  }
  return false;
}

}  // namespace bustub
