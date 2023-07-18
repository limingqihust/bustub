/**
 * deadlock_detection_test.cpp
 */

#include <atomic>
#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace bustub {
TEST(LockManagerDeadlockDetectionTest, EdgeTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;
  lock_mgr.StartDeadlockDetection();

  const int num_nodes = 100;
  const int num_edges = num_nodes / 2;
  const int seed = 15445;
  std::srand(seed);

  // Create txn ids and shuffle
  std::vector<txn_id_t> txn_ids;
  txn_ids.reserve(num_nodes);
  for (int i = 0; i < num_nodes; i++) {
    txn_ids.push_back(i);
  }
  EXPECT_EQ(num_nodes, txn_ids.size());
  auto rng = std::default_random_engine{};
  std::shuffle(txn_ids.begin(), txn_ids.end(), rng);
  EXPECT_EQ(num_nodes, txn_ids.size());

  // Create edges by pairing adjacent txn_ids
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (int i = 0; i < num_nodes; i += 2) {
    EXPECT_EQ(i / 2, lock_mgr.GetEdgeList().size());
    auto t1 = txn_ids[i];
    auto t2 = txn_ids[i + 1];
    lock_mgr.AddEdge(t1, t2);
    edges.emplace_back(t1, t2);
    EXPECT_EQ((i / 2) + 1, lock_mgr.GetEdgeList().size());
  }

  auto lock_mgr_edges = lock_mgr.GetEdgeList();
  EXPECT_EQ(num_edges, lock_mgr_edges.size());
  EXPECT_EQ(num_edges, edges.size());

  std::sort(lock_mgr_edges.begin(), lock_mgr_edges.end());
  std::sort(edges.begin(), edges.end());

  for (int i = 0; i < num_edges; i++) {
    EXPECT_EQ(edges[i], lock_mgr_edges[i]);
  }
}

TEST(LockManagerDeadlockDetectionTest, CycleTest1) {
  LockManager lock_mgr{};
  lock_mgr.AddEdge(0, 1);
  lock_mgr.AddEdge(1, 0);
  lock_mgr.AddEdge(2, 3);
  lock_mgr.AddEdge(3, 4);
  lock_mgr.AddEdge(4, 2);
  txn_id_t txn_id;
  lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(1, txn_id);
  lock_mgr.RemoveEdge(1, 0);
  lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(4, txn_id);
  lock_mgr.RemoveEdge(4, 2);
}

TEST(LockManagerDeadlockDetectionTest, GraphTest) {
  LockManager lock_mgr{};
  lock_mgr.AddEdge(0, 1);
  lock_mgr.AddEdge(1, 2);
  lock_mgr.AddEdge(2, 3);
  lock_mgr.AddEdge(3, 4);
  lock_mgr.AddEdge(4, 5);
  lock_mgr.AddEdge(5, 0);
  LOG_INFO("# EdgeList : ");
  for (auto edge : lock_mgr.GetEdgeList()) {
    LOG_INFO("# %d %d", edge.first, edge.second);
  }

  txn_id_t txn_id;
  lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(5, txn_id);

  lock_mgr.RemoveEdge(5, 0);

  LOG_INFO("# EdgeList : ");
  for (auto edge : lock_mgr.GetEdgeList()) {
    LOG_INFO("# %d %d", edge.first, edge.second);
  }
  lock_mgr.AddEdge(2, 6);
  lock_mgr.AddEdge(6, 7);
  lock_mgr.AddEdge(7, 2);
  LOG_INFO("# EdgeList : ");
  for (auto edge : lock_mgr.GetEdgeList()) {
    LOG_INFO("# %d %d", edge.first, edge.second);
  }
  lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(7, txn_id);

  lock_mgr.RemoveEdge(7, 2);

  LOG_INFO("# EdgeList : ");
  for (auto edge : lock_mgr.GetEdgeList()) {
    LOG_INFO("# %d %d", edge.first, edge.second);
  }
  lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(INVALID_TXN_ID, txn_id);
  EXPECT_EQ(lock_mgr.GetEdgeList().size(), 7);
}

TEST(LockManagerDeadlockDetectionTest, BasicDeadlockDetectionTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;
  lock_mgr.StartDeadlockDetection();

  table_oid_t toid{0};
  RID rid0{0, 0};
  RID rid1{1, 1};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // This will block
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);

    lock_mgr.UnlockRow(txn0, toid, rid1);
    lock_mgr.UnlockRow(txn0, toid, rid0);
    lock_mgr.UnlockTable(txn0, toid);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());

    // This will block
    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
    txn_mgr.Abort(txn1);
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();

  delete txn0;
  delete txn1;
}

TEST(LockManagerDeadlockDetectionTest, BasicDeadlockDetectionTest1) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;
  lock_mgr.StartDeadlockDetection();

  table_oid_t toid{0};
  RID rid0{0, 0};
  RID rid1{1, 1};
  RID rid2{2, 2};
  RID rid3{3, 3};
  RID rid4{4, 4};
  RID rid5{5, 5};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  auto *txn3 = txn_mgr.Begin();
  auto *txn4 = txn_mgr.Begin();
  auto *txn5 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());
  EXPECT_EQ(2, txn2->GetTransactionId());
  EXPECT_EQ(3, txn3->GetTransactionId());
  EXPECT_EQ(4, txn4->GetTransactionId());
  EXPECT_EQ(5, txn5->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn0->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // This will block
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);

    lock_mgr.UnlockRow(txn0, toid, rid1);
    lock_mgr.UnlockRow(txn0, toid, rid0);
    lock_mgr.UnlockTable(txn0, toid);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // This will block
    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid2);
    EXPECT_EQ(res, true);

    lock_mgr.UnlockRow(txn1, toid, rid2);
    lock_mgr.UnlockRow(txn1, toid, rid1);
    lock_mgr.UnlockTable(txn1, toid);

    txn_mgr.Commit(txn1);
    EXPECT_EQ(TransactionState::COMMITTED, txn1->GetState());
  });

  std::thread t2([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    bool res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid, rid2);
    EXPECT_EQ(TransactionState::GROWING, txn2->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    // This will block
    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn2->GetState());
    txn_mgr.Abort(txn2);
  });

  std::thread t3([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    bool res = lock_mgr.LockTable(txn3, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn3, LockManager::LockMode::EXCLUSIVE, toid, rid3);
    EXPECT_EQ(TransactionState::GROWING, txn3->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // This will block
    res = lock_mgr.LockRow(txn3, LockManager::LockMode::EXCLUSIVE, toid, rid4);
    EXPECT_EQ(res, true);

    lock_mgr.UnlockRow(txn3, toid, rid4);
    lock_mgr.UnlockRow(txn3, toid, rid3);
    lock_mgr.UnlockTable(txn3, toid);

    txn_mgr.Commit(txn3);
    EXPECT_EQ(TransactionState::COMMITTED, txn3->GetState());
  });

  std::thread t4([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    bool res = lock_mgr.LockTable(txn4, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn4, LockManager::LockMode::EXCLUSIVE, toid, rid4);
    EXPECT_EQ(TransactionState::GROWING, txn4->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // This will block
    res = lock_mgr.LockRow(txn4, LockManager::LockMode::EXCLUSIVE, toid, rid5);
    EXPECT_EQ(res, true);

    lock_mgr.UnlockRow(txn4, toid, rid5);
    lock_mgr.UnlockRow(txn4, toid, rid4);
    lock_mgr.UnlockTable(txn4, toid);

    txn_mgr.Commit(txn4);
    EXPECT_EQ(TransactionState::COMMITTED, txn4->GetState());
  });

  std::thread t5([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    bool res = lock_mgr.LockTable(txn5, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn5, LockManager::LockMode::EXCLUSIVE, toid, rid5);
    EXPECT_EQ(TransactionState::GROWING, txn5->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    // This will block
    res = lock_mgr.LockRow(txn5, LockManager::LockMode::EXCLUSIVE, toid, rid3);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn5->GetState());
    txn_mgr.Abort(txn5);
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();
  t2.join();
  t3.join();
  t4.join();
  t5.join();
  delete txn0;
  delete txn1;
  delete txn2;
  delete txn3;
  delete txn4;
  delete txn5;
}

TEST(LockManagerDeadlockDetectionTest, CyclesTest1) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;
  lock_mgr.StartDeadlockDetection();

  table_oid_t toid0 = 0;
  table_oid_t toid1 = 1;
  table_oid_t toid2 = 2;
  RID rid0{0, 0};
  RID rid1{1, 1};
  RID rid2{2, 2};
  RID rid3{3, 3};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  auto *txn3 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());
  EXPECT_EQ(2, txn2->GetTransactionId());
  EXPECT_EQ(3, txn3->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, toid0);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::SHARED, toid0, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn0->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // This will block
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, toid1);
    EXPECT_EQ(true, res);
    lock_mgr.UnlockRow(txn0, toid0, rid0);
    lock_mgr.UnlockTable(txn0, toid0);
    lock_mgr.UnlockTable(txn0, toid1);
    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, toid1);
    EXPECT_EQ(res, true);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // This will block
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, toid2);
    EXPECT_EQ(res, true);

    lock_mgr.UnlockTable(txn1, toid1);
    lock_mgr.UnlockTable(txn1, toid2);

    txn_mgr.Commit(txn1);
    EXPECT_EQ(TransactionState::COMMITTED, txn1->GetState());
  });

  std::thread t2([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    bool res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_EXCLUSIVE, toid2);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid2, rid2);
    EXPECT_EQ(res, true);
    EXPECT_EQ(TransactionState::GROWING, txn2->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // This will block
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::SHARED, toid1);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn2->GetState());
    txn_mgr.Abort(txn2);
  });

  std::thread t3([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    bool res = lock_mgr.LockTable(txn3, LockManager::LockMode::INTENTION_EXCLUSIVE, toid2);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn3, LockManager::LockMode::EXCLUSIVE, toid2, rid3);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockTable(txn3, LockManager::LockMode::INTENTION_EXCLUSIVE, toid0);
    EXPECT_EQ(res, true);
    EXPECT_EQ(TransactionState::GROWING, txn3->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // This will block
    res = lock_mgr.LockRow(txn3, LockManager::LockMode::EXCLUSIVE, toid0, rid0);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn3->GetState());
    txn_mgr.Abort(txn3);
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();
  t2.join();
  t3.join();
  delete txn0;
  delete txn1;
  delete txn2;
  delete txn3;
}

}  // namespace bustub
