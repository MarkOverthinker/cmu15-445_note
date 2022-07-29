//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> ul(latch_);
  while (true) {
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING || txn->GetState() == TransactionState::COMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    LockRequestQueue &lrq = lock_table_[rid];
    bool flag = true;
    auto it = lrq.request_queue_.begin();
    while (it != lrq.request_queue_.end()) {
      if (it->lock_mode_ == LockMode::EXCLUSIVE) {
        if (it->txn_id_ > txn->GetTransactionId()) {
          Transaction *tran = TransactionManager::GetTransaction(it->txn_id_);
          it = lrq.request_queue_.erase(it);
          tran->GetExclusiveLockSet()->erase(rid);
          tran->GetSharedLockSet()->erase(rid);
          tran->SetState(TransactionState::ABORTED);
          lrq.cv_.notify_all();
          continue;
        }
        if (it->txn_id_ < txn->GetTransactionId()) {
          flag = false;
        }
      }
      ++it;
    }
    std::list<LockRequest>::iterator place = lrq.Insert(txn->GetTransactionId(), LockMode::SHARED);
    if (flag) {
      txn->SetState(TransactionState::GROWING);
      txn->GetSharedLockSet()->emplace(rid);
      place->granted_ = true;
      return true;
    }
    lrq.cv_.wait(ul);
  }
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> ul(latch_);
  while (true) {
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    if (txn->GetState() == TransactionState::SHRINKING || txn->GetState() == TransactionState::COMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    LockRequestQueue &lrq = lock_table_[rid];
    auto it = lrq.request_queue_.begin();
    while (it != lrq.request_queue_.end()) {
      if (it->txn_id_ > txn->GetTransactionId()) {
        Transaction *tran = TransactionManager::GetTransaction(it->txn_id_);
        it = lrq.request_queue_.erase(it);
        tran->GetExclusiveLockSet()->erase(rid);
        tran->GetSharedLockSet()->erase(rid);
        tran->SetState(TransactionState::ABORTED);
        lrq.cv_.notify_all();
        continue;
      }
      ++it;
    }
    std::list<LockRequest>::iterator place = lrq.Insert(txn->GetTransactionId(), LockMode::EXCLUSIVE);
    if (place == lrq.request_queue_.begin()) {
      txn->SetState(TransactionState::GROWING);
      txn->GetExclusiveLockSet()->emplace(rid);
      place->granted_ = true;
      return true;
    }
    lrq.cv_.wait(ul);
  }
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> ul(latch_);
  while (true) {
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    if (txn->GetState() == TransactionState::SHRINKING || txn->GetState() == TransactionState::COMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    LockRequestQueue &lrq = lock_table_[rid];
    if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
      std::list<LockRequest>::iterator place = lrq.Insert(txn->GetTransactionId(), LockMode::SHARED);
      if (!place->granted_) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      lrq.request_queue_.erase(place);
      auto it = lrq.request_queue_.begin();
      while (it != lrq.request_queue_.end()) {
        if (!it->granted_) {
          break;
        }
        ++it;
      }
      lrq.request_queue_.insert(it, LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
      txn->GetSharedLockSet()->erase(rid);
    }
    auto it = lrq.request_queue_.begin();
    while (it != lrq.request_queue_.end()) {
      if (it->txn_id_ > txn->GetTransactionId()) {
        Transaction *tran = TransactionManager::GetTransaction(it->txn_id_);
        it = lrq.request_queue_.erase(it);
        tran->GetExclusiveLockSet()->clear();
        tran->GetSharedLockSet()->clear();
        tran->SetState(TransactionState::ABORTED);
        lrq.cv_.notify_all();
        continue;
      }
      ++it;
    }
    if (lrq.request_queue_.front().txn_id_ == txn->GetTransactionId()) {
      txn->SetState(TransactionState::GROWING);
      txn->GetExclusiveLockSet()->emplace(rid);
      lrq.request_queue_.front().granted_ = true;
      return true;
    }
    lrq.cv_.wait(ul);
  }
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> ul(latch_);
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  LockRequestQueue &lrq = lock_table_[rid];
  for (auto it = lrq.request_queue_.begin(); it != lrq.request_queue_.end(); ++it) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->erase(rid);
      lrq.request_queue_.erase(it);
      lrq.cv_.notify_all();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
