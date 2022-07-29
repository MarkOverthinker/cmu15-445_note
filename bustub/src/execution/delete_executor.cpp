//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  child_executor_->Init();
  try {
    Tuple tmptuple;
    Transaction *txn = exec_ctx_->GetTransaction();
    LockManager *lckm = exec_ctx_->GetLockManager();
    while (child_executor_->Next(&tmptuple, rid)) {
      if (lckm != nullptr) {
        if (txn->IsSharedLocked(*rid)) {
          lckm->LockUpgrade(txn, *rid);
        } else {
          lckm->LockExclusive(txn, *rid);
        }
      }
      table_heap_->MarkDelete(*rid, exec_ctx_->GetTransaction());
      for (auto index : indexes_) {
        index->index_->DeleteEntry(
            tmptuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
            *rid, exec_ctx_->GetTransaction());
        txn->GetIndexWriteSet()->emplace_back(IndexWriteRecord(*rid, table_info_->oid_, WType::DELETE, tmptuple, *tuple,
                                                               index->index_oid_, exec_ctx_->GetCatalog()));
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lckm != nullptr) {
        lckm->Unlock(txn, *rid);
      }
    }
  } catch (Exception &e) {
    throw Exception(e.GetType(), "exception in delete_executor");
  }
  return false;
}

}  // namespace bustub
