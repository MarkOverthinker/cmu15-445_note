//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  child_executor_->Init();
  try {
    Tuple tmptuple;
    Transaction *txn = exec_ctx_->GetTransaction();
    LockManager *lckm = exec_ctx_->GetLockManager();
    while (child_executor_->Next(tuple, rid)) {
      if (lckm != nullptr) {
        if (txn->IsSharedLocked(*rid)) {
          lckm->LockUpgrade(txn, *rid);
        } else {
          lckm->LockExclusive(txn, *rid);
        }
      }
      tmptuple = GenerateUpdatedTuple(*tuple);
      table_heap_->UpdateTuple(tmptuple, *rid, exec_ctx_->GetTransaction());
      for (auto index : indexes_) {
        index->index_->DeleteEntry(
            tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
            *rid, exec_ctx_->GetTransaction());
        index->index_->InsertEntry(
            tmptuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
            *rid, exec_ctx_->GetTransaction());
        txn->GetIndexWriteSet()->emplace_back(IndexWriteRecord(*rid, table_info_->oid_, WType::UPDATE, tmptuple, *tuple,
                                                               index->index_oid_, exec_ctx_->GetCatalog()));
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lckm != nullptr) {
        lckm->Unlock(txn, *rid);
      }
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }
  return false;
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
