/**
 * lock_manager_test.cpp
 */

#include <atomic>
#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "catalog/table_generator.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/insert_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "gtest/gtest.h"
#include "test_util.h"  // NOLINT
#include "type/value_factory.h"

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}

// Basic shared lock test under REPEATABLE_READ
void BasicTest1() {
  // LockManager lock_mgr{};
  // TransactionManager txn_mgr{&lock_mgr};

  // std::vector<RID> rids;
  // std::vector<Transaction *> txns;
  // int num_rids = 10;
  // for (int i = 0; i < num_rids; i++) {
  //   RID rid{i, static_cast<uint32_t>(i)};
  //   rids.push_back(rid);
  //   txns.push_back(txn_mgr.Begin());
  //   EXPECT_EQ(i, txns[i]->GetTransactionId());
  // }
  // // test

  // auto task = [&](int txn_id) {
  //   bool res;
  //   for (const RID &rid : rids) {
  //     res = lock_mgr.LockShared(txns[txn_id], rid);
  //     EXPECT_TRUE(res);
  //     CheckGrowing(txns[txn_id]);
  //   }
  //   for (const RID &rid : rids) {
  //     res = lock_mgr.Unlock(txns[txn_id], rid);
  //     EXPECT_TRUE(res);
  //     CheckShrinking(txns[txn_id]);
  //   }
  //   txn_mgr.Commit(txns[txn_id]);
  //   CheckCommitted(txns[txn_id]);
  // };
  // std::vector<std::thread> threads;
  // threads.reserve(num_rids);

  // for (int i = 0; i < num_rids; i++) {
  //   threads.emplace_back(std::thread{task, i});
  // }

  // for (int i = 0; i < num_rids; i++) {
  //   threads[i].join();
  // }

  // for (int i = 0; i < num_rids; i++) {
  //   delete txns[i];
  // }


  
  //   LockManager lock_mgr{};
  // TransactionManager txn_mgr{&lock_mgr};
  // RID rid0{0, 0};
  // RID rid1{0, 1};

  // std::mutex id_mutex;
  // size_t id_hold = 0;
  // size_t id_wait = 10;
  // size_t id_kill = 1;

  // size_t num_wait = 10;
  // size_t num_kill = 1;

  // std::vector<std::thread> wait_threads;
  // std::vector<std::thread> kill_threads;

  // Transaction txn(id_hold);
  // txn_mgr.Begin(&txn);
  // lock_mgr.LockExclusive(&txn, rid0);
  // lock_mgr.LockShared(&txn, rid1);
  // auto wait_die_task = [&]() {
  //   id_mutex.lock();
  //   Transaction wait_txn(id_wait++);
  //   id_mutex.unlock();
  //   bool res;
  //   txn_mgr.Begin(&wait_txn);
  //   res = lock_mgr.LockShared(&wait_txn, rid1);
  //   EXPECT_TRUE(res);
  //   CheckGrowing(&wait_txn);
  //   CheckTxnLockSize(&wait_txn, 1, 0);
  //   try {
  //     res = lock_mgr.LockExclusive(&wait_txn, rid0);
  //     EXPECT_FALSE(res) << wait_txn.GetTransactionId() << "ERR";
  //   } catch (const TransactionAbortException &e) {
  //   } catch (const Exception &e) {
  //     EXPECT_TRUE(false) << "Test encountered exception" << e.what();
  //   }

  //   CheckAborted(&wait_txn);

  //   txn_mgr.Abort(&wait_txn);
  // };

  // // All transaction here should wait.
  // for (size_t i = 0; i < num_wait; i++) {
  //   wait_threads.emplace_back(std::thread{wait_die_task});
  //   std::this_thread::sleep_for(std::chrono::milliseconds(50));
  // }

  // // TODO(peijingx): guarantee all are waiting on LockExclusive
  // std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // auto kill_task = [&]() {
  //   Transaction kill_txn(id_kill++);

  //   bool res;
  //   txn_mgr.Begin(&kill_txn);
  //   res = lock_mgr.LockShared(&kill_txn, rid1);
  //   EXPECT_TRUE(res);
  //   CheckGrowing(&kill_txn);
  //   CheckTxnLockSize(&kill_txn, 1, 0);

  //   res = lock_mgr.LockShared(&kill_txn, rid0);
  //   EXPECT_TRUE(res);
  //   CheckGrowing(&kill_txn);
  //   CheckTxnLockSize(&kill_txn, 2, 0);
  //   txn_mgr.Commit(&kill_txn);
  //   CheckCommitted(&kill_txn);
  //   CheckTxnLockSize(&kill_txn, 0, 0);
  // };

  // for (size_t i = 0; i < num_kill; i++) {
  //   kill_threads.emplace_back(std::thread{kill_task});
  // }

  // for (size_t i = 0; i < num_wait; i++) {
  //   wait_threads[i].join();
  // }

  // CheckGrowing(&txn);
  // txn_mgr.Commit(&txn);
  // CheckCommitted(&txn);

  // for (size_t i = 0; i < num_kill; i++) {
  //   kill_threads[i].join();
  // }




  // txn1: DELETE FROM test_1 WHERE colA < 50
  // txn1: abort
  // txn2: SELECT colA FROM test_1 WHERE colA < 50

  // Construct query plan
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
  auto &schema = table_info->schema_;
  auto col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto const50 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(50));
  auto predicate = MakeComparisonExpression(col_a, const50, ComparisonType::LessThan);
  auto out_schema1 = MakeOutputSchema({{"colA", col_a}});
  auto scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, predicate, table_info->oid_);
  // index
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto index_info = GetExecutorContext()->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      GetTxn(), "index1", "test_1", GetExecutorContext()->GetCatalog()->GetTable("test_1")->schema_, *key_schema, {0},
      8, HashFunction<GenericKey<8>>{});

  auto txn1 = GetTxnManager()->Begin();
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  std::unique_ptr<AbstractPlanNode> delete_plan;
  { delete_plan = std::make_unique<DeletePlanNode>(scan_plan1.get(), table_info->oid_); }
  GetExecutionEngine()->Execute(delete_plan.get(), nullptr, txn1, exec_ctx1.get());
  GetTxnManager()->Abort(txn1);
  delete txn1;

  auto txn2 = GetTxnManager()->Begin();
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(scan_plan1.get(), &result_set, txn2, exec_ctx2.get());

  // Verify
  for (const auto &tuple : result_set) {
    ASSERT_TRUE(tuple.GetValue(out_schema1, out_schema1->GetColIdx("colA")).GetAs<int32_t>() < 50);
  }
  ASSERT_EQ(result_set.size(), 50);
  Tuple index_key = Tuple(result_set[0]);

  std::vector<RID> rids;

  index_info->index_->ScanKey(index_key, &rids, txn2);
  ASSERT_TRUE(!rids.empty());

  GetTxnManager()->Commit(txn2);
  delete txn2;
}
TEST(LockManagerTest, BasicTest) { BasicTest1(); }

void TwoPLTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid0{0, 0};
  RID rid1{0, 1};

  auto txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  bool res;
  res = lock_mgr.LockShared(txn, rid0);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 0);

  res = lock_mgr.LockExclusive(txn, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 1);

  res = lock_mgr.Unlock(txn, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnLockSize(txn, 0, 1);

  try {
    lock_mgr.LockShared(txn, rid0);
    CheckAborted(txn);
    // Size shouldn't change here
    CheckTxnLockSize(txn, 0, 1);
  } catch (TransactionAbortException &e) {
    // std::cout << e.GetInfo() << std::endl;
    CheckAborted(txn);
    // Size shouldn't change here
    CheckTxnLockSize(txn, 0, 1);
  }

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnLockSize(txn, 0, 0);

  delete txn;
}
TEST(LockManagerTest, TwoPLTest) { TwoPLTest(); }

void UpgradeTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  Transaction txn(0);
  txn_mgr.Begin(&txn);

  bool res = lock_mgr.LockShared(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 1, 0);
  CheckGrowing(&txn);

  res = lock_mgr.LockUpgrade(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 0, 1);
  CheckGrowing(&txn);

  res = lock_mgr.Unlock(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 0, 0);
  CheckShrinking(&txn);

  txn_mgr.Commit(&txn);
  CheckCommitted(&txn);
}
TEST(LockManagerTest, UpgradeLockTest) { UpgradeTest(); }

void WoundWaitBasicTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  int id_hold = 0;
  int id_die = 1;

  std::promise<void> t1done;
  std::shared_future<void> t1_future(t1done.get_future());

  auto wait_die_task = [&]() {
    // younger transaction acquires lock first
    Transaction txn_die(id_die);
    txn_mgr.Begin(&txn_die);
    bool res = lock_mgr.LockExclusive(&txn_die, rid);
    EXPECT_TRUE(res);

    CheckGrowing(&txn_die);
    CheckTxnLockSize(&txn_die, 0, 1);

    t1done.set_value();

    // wait for txn 0 to call lock_exclusive(), which should wound us
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    CheckAborted(&txn_die);

    // unlock
    txn_mgr.Abort(&txn_die);
  };

  Transaction txn_hold(id_hold);
  txn_mgr.Begin(&txn_hold);

  // launch the waiter thread
  std::thread wait_thread{wait_die_task};

  // wait for txn1 to lock
  t1_future.wait();

  bool res = lock_mgr.LockExclusive(&txn_hold, rid);
  EXPECT_TRUE(res);

  wait_thread.join();

  CheckGrowing(&txn_hold);
  txn_mgr.Commit(&txn_hold);
  CheckCommitted(&txn_hold);
}
TEST(LockManagerTest, WoundWaitBasicTest) { WoundWaitBasicTest(); }

}  // namespace bustub
