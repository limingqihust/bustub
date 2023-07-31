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

#include "concurrency/lock_manager.h"
#include <stack>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

/*
 * 1 检查事务隔离级别下是否支持该锁
 * 2 获取lock_request_queue
 * 3 遍历lock_request_queue 如果该txn对该table有过锁请求
 *    3.1 如果是相同类型的锁请求 该txn已经获取锁 返回true
 *    3.2 如果是不同类型的锁请求 那么需要升级 首先保证这个table没有其他txn的升级请求
 *    3.3 检查锁升级操作是否合法
 *    3.4 进行锁升级操作 首先删除旧的请求 插入新的锁升级请求 锁升级请求优先级更高
 *    3.5 尝试获取锁
 * 4 lock_request_queue没有该事务的请求 声明一个锁请求 并插入到lock_request_queue的队尾
 * 5 尝试获取锁
 */
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
//  LOG_INFO("# LockTable : txn %d try to lock table %d with %s", txn->GetTransactionId(), oid,
//           fmt::format("lock mode : {}", lock_mode).c_str());
  // 检查是不是符合隔离级别
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  // 获取该table的LockRequestQueue
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }

  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  for (const auto &lock_request : lock_request_queue->request_queue_) {
    // 该txn对该table已经有了一个锁了 尝试升级锁
    // 这个锁一定是获取了的 因为不可能正在请求一个锁还去申请另一个锁 并且对同一个表只可能有一个锁 不可能同时持有两把锁
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      return UpgradeLockTable(txn, lock_request_queue, lock_mode, oid);
    }
  }
  // 该txn对该table不持有锁 新建一个锁请求
  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.emplace_back(new_lock_request);
  // 尝试获取锁
  while (!GrantLockIfPossible(new_lock_request, lock_request_queue)) {
//    LOG_INFO("# LockTable : txn %d lock table %d is blocked", txn->GetTransactionId(), oid);
    lock_request_queue->cv_.wait(lock);
//    LOG_INFO("# LockTable : txn %d is awaken from blocking, try to lock table %d", txn->GetTransactionId(), oid);
    if (txn->GetState() == TransactionState::ABORTED) {
//      LOG_INFO("# LockTable : txn %d is aborted, notify all thread blocked by table %d", txn->GetTransactionId(), oid);
      lock_request_queue->request_queue_.remove(new_lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  // 成功获取锁
//  LOG_INFO("# LockTable : txn %d successfully get lock of table %d", txn->GetTransactionId(), oid);
  new_lock_request->granted_ = true;
  InsertTableLock(txn, oid, lock_mode);
  lock_request_queue->cv_.notify_all();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
//  LOG_INFO("# UnlockTable : txn %d unlock table %d", txn->GetTransactionId(), oid);
  // 检查该txn是否持有该表的行级锁
  if (!EasureUnlockRowBeforeUnlockTable(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }
  table_lock_map_latch_.lock();

  // 没有对该table的锁请求队列
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  auto lock_request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  // 寻找该txn对该table的锁请求
  for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
       iter++) {
    auto lock_request = *iter;
    // 找到该txn对该table的锁请求 成功释放 并唤醒其他等待该table的锁的线程
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      DeleteTableLock(txn, oid, lock_request->lock_mode_);
      // 修改该txn的状态
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::REPEATABLE_READ:
          if (lock_request->lock_mode_ == LockMode::EXCLUSIVE || lock_request->lock_mode_ == LockMode::SHARED) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::READ_COMMITTED:
        case IsolationLevel::READ_UNCOMMITTED:
          if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        default:
          break;
      }
      // 从该table的锁请求队列中删除该txn对该锁的请求
      lock_request_queue->request_queue_.erase(iter);
      lock_request_queue->cv_.notify_all();
      return true;
    }
  }

  // 该事务不对该table持有任何锁
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

/*
 * 1 检查行级锁不支持意向锁
 * 2 检查事务隔离级别是否支持该锁
 * 3 加行级锁前需要保证表级锁
 * 4 获取lock_request_queue
 * 5 遍历lock_request_queue
 * 6 如果找到该事务对该锁的请求（一定是授予的）
 *    6.1 如果是同一级别 直接返回
 *    6.2 如果不是同一级别 保证此时没有其他事务对该锁升级的请求
 *    6.3 检查是否可以升级
 *    6.4 删除就请求 插入新请求 锁升级请求优先
 *    6.5 尝试获取锁
 * 7 如果没有找到 新建一个请求
 *    7.1 插入到请求队列中 插入到队尾
 *    7.2 尝试获取
 *
 */
auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
//  LOG_INFO("# LockRow : txn %d try to lock table %d row <%d %d> with %s", txn->GetTransactionId(), oid, rid.GetPageId(),
//           rid.GetSlotNum(), fmt::format("lock mode : {}", lock_mode).c_str());
  // 行级锁不支持意向锁
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }
  // 检查事务隔离级别是否支持该锁
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }
  // 检查加行级锁前是否加了表级锁
  if (!EasureTableLockBeforeRowLock(txn, lock_mode, oid)) {
    return false;
  }
  // 获取LockRequestQueue
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  for (const auto &lock_request : lock_request_queue->request_queue_) {
    // 该txn对该rid已经有了一个锁了 尝试升级锁
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      return UpgradeLockRow(txn, lock_request_queue, lock_mode, oid, rid);
    }
  }
  // 该txn对该rid没有锁 新建一个
  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.emplace_back(new_lock_request);

  // 尝试获取锁
  while (!GrantLockIfPossible(new_lock_request, lock_request_queue)) {
//    LOG_INFO("# LockRow : txn %d lock rid <%d %d> is blocked", txn->GetTransactionId(), rid.GetPageId(),
//             rid.GetSlotNum());
    lock_request_queue->cv_.wait(lock);
//    LOG_INFO("# LockRow : txn %d is awaken from blocking, try to lock row <%d %d>", txn->GetTransactionId(),
//             rid.GetPageId(), rid.GetSlotNum());
    if (txn->GetState() == TransactionState::ABORTED) {
//      LOG_INFO("# LockRow : txn %d is aborted, notify thread blocked by row <%d %d>", txn->GetTransactionId(),
//               rid.GetPageId(), rid.GetSlotNum());
      lock_request_queue->request_queue_.remove(new_lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  // 成功获取锁
//  LOG_INFO("# LockRow : txn %d successfully get lock of row <%d %d>", txn->GetTransactionId(), rid.GetPageId(),
//           rid.GetSlotNum());
  new_lock_request->granted_ = true;
  InsertRowLock(txn, oid, rid, lock_mode);
  lock_request_queue->cv_.notify_all();
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
//  LOG_INFO("# UnlockRow : txn %d unlock row <%d %d>", txn->GetTransactionId(), rid.GetPageId(), rid.GetSlotNum());
  row_lock_map_latch_.lock();

  // 如果没有对该rid的锁请求队列
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  auto lock_request_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  // 寻找该txn对该rid的锁请求
  for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
       iter++) {
    auto lock_request = *iter;
    // 找到了该txn对该rid的锁请求 成功释放 并唤醒其他等待该rid锁的线程
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      DeleteRowLock(txn, oid, rid, lock_request->lock_mode_);

      // 修该该txn的状态
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::REPEATABLE_READ:
          if (lock_request->lock_mode_ == LockMode::EXCLUSIVE || lock_request->lock_mode_ == LockMode::SHARED) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::READ_COMMITTED:
        case IsolationLevel::READ_UNCOMMITTED:
          if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        default:
          break;
      }

      // 从该rid的锁请求队列中删除该请求
      lock_request_queue->request_queue_.erase(iter);
//      LOG_INFO("# UnlockRow : txn %d notify txn blocked by row <%d %d>", txn->GetTransactionId(), rid.GetPageId(),
//               rid.GetSlotNum());
      lock_request_queue->cv_.notify_all();
      return true;
    }
  }

  // 该事务对该rid不持有任何锁
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

/*
 * 升级一个表级锁
 * 已经获取了lock_request_queue的锁
 */
auto LockManager::UpgradeLockTable(Transaction *txn, std::shared_ptr<LockRequestQueue> &lock_request_queue,
                                   LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 找到该table的锁请求序列
  for (const auto &lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    // 找到该txn对该table的锁的一个请求
    if (lock_request->lock_mode_ == lock_mode) {  // 如果已获得对该table的同级锁
                                                  //      lock_request_queue->latch_.unlock();
      return true;
    }

    // 有其他txn对该table有一个锁升级请求 不合法
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      return false;
    }

    // 检查锁升级操作是否合法
    if (!CanLockUpgrade(lock_request->lock_mode_, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    // 下面进行锁升级操作
    // 删除旧的锁请求
    DeleteTableLock(txn, oid, lock_request->lock_mode_);
    lock_request_queue->request_queue_.remove(lock_request);

    // 插入新的锁请求 锁升级请求优先
    auto new_upgrading_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    auto insert_pos = lock_request_queue->request_queue_.begin();
    for (; insert_pos != lock_request_queue->request_queue_.end(); insert_pos++) {
      if (!(*insert_pos)->granted_) {
        break;
      }
    }

    lock_request_queue->request_queue_.insert(insert_pos, new_upgrading_lock_request);
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
    while (!GrantLockIfPossible(new_upgrading_lock_request, lock_request_queue)) {
//      LOG_INFO("# UpgradeTableLock : txn %d upgrade lock table %d with %s is blocked", txn->GetTransactionId(), oid,
//               fmt::format("lock mode {}", lock_mode).c_str());
      lock_request_queue->cv_.wait(lock);
//      LOG_INFO("# UpgradeTableLock : txn %d is awaken from blocking, try to lock table %d with %s",
//               txn->GetTransactionId(), oid, fmt::format("lock mode {}", lock_mode).c_str());
      if (txn->GetState() == TransactionState::ABORTED) {
//        LOG_INFO("# UpgradeTableLock : txn %d is aborted, notify all thread blocked by table %d",
//                 txn->GetTransactionId(), oid);
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        lock_request_queue->request_queue_.remove(new_upgrading_lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
      }
//      LOG_INFO("# UpgradeTableLock : txn %d try to upgrade lock table %d with %s", txn->GetTransactionId(), oid,
//               fmt::format("lock mode {}", lock_mode).c_str());
    }

    // 完成升级锁请求
//    LOG_INFO("# UpgradeLockTable : txn %d successfully upgrade lock on table %d with %s", txn->GetTransactionId(), oid,
//             fmt::format("lock mod {}", lock_mode).c_str());
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    new_upgrading_lock_request->granted_ = true;
    InsertTableLock(txn, oid, new_upgrading_lock_request->lock_mode_);

    lock_request_queue->cv_.notify_all();

    return true;
  }
  return false;
}

/*
 * 升级一个行级锁
 */
auto LockManager::UpgradeLockRow(Transaction *txn, std::shared_ptr<LockRequestQueue> &lock_request_queue,
                                 LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 找到该rid的锁请求队列
  for (const auto &lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    // 找到该txn对该rid的一个锁请求
    if (lock_request->lock_mode_ == lock_mode) {
      return true;
    }

    // 有其他txn对该rid的一个锁升级请求 不合法
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      return false;
    }

    // 检查锁升级操作是否合法
    if (!CanLockUpgrade(lock_request->lock_mode_, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    // 下面进行锁升级操作
    // 删除旧的锁请求
    DeleteRowLock(txn, oid, rid, lock_request->lock_mode_);
    lock_request_queue->request_queue_.remove(lock_request);

    // 插入新的锁升级请求 锁升级请求优先
    auto new_upgrading_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    auto insert_pos = lock_request_queue->request_queue_.begin();
    for (; insert_pos != lock_request_queue->request_queue_.end(); insert_pos++) {
      if (!(*insert_pos)->granted_) {
        break;
      }
    }
    lock_request_queue->request_queue_.insert(insert_pos, new_upgrading_lock_request);
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
    while (!GrantLockIfPossible(new_upgrading_lock_request, lock_request_queue)) {
//      LOG_INFO("# UpgradeRowLock : txn %d upgrade lock on row %s with %s is blocked", txn->GetTransactionId(),
//               rid.ToString().c_str(), fmt::format("lock mode {}", lock_mode).c_str());
      lock_request_queue->cv_.wait(lock);
//      LOG_INFO("# LockRow : txn %d is awaken from blocking, try to lock row %s with %s", txn->GetTransactionId(),
//               rid.ToString().c_str(), fmt::format("lock mode {}", lock_mode).c_str());
      if (txn->GetState() == TransactionState::ABORTED) {
//        LOG_INFO("# UpgradeRowLock : txn %d is aborted, notify all thread blocked by row %s", txn->GetTransactionId(),
//                 rid.ToString().c_str());
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        lock_request_queue->request_queue_.remove(new_upgrading_lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
      }
//      LOG_INFO("# UpgradeRowLock : txn %d try to upgrade lock on row %s with %s", txn->GetTransactionId(),
//               rid.ToString().c_str(), fmt::format("lock mode {}", lock_mode).c_str());
    }

    // 完成锁升级请求
//    LOG_INFO("# UpgradeRowLock : txn %d successfully upgrade lock on row %s with %s", txn->GetTransactionId(),
//             rid.ToString().c_str(), fmt::format("lock mod {}", lock_mode).c_str());
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    new_upgrading_lock_request->granted_ = true;
    InsertRowLock(txn, oid, rid, new_upgrading_lock_request->lock_mode_);
    lock_request_queue->cv_.notify_all();
    return true;
  }
  return false;
}

/*
 * 隔离级别下是否支持加锁
 * REPEATABLE_READ隔离级别下，SHRINKING状态下,不允许任何锁
 * READ_COMMITTED隔离级别下，SHRINKING状态下，只允许S/IS锁,不允许X/IX/SIX锁
 * READ_UNCOMMITTED隔离级别下，只支持X和IX锁 不支持S/IS/SIX锁
 * SHRINKING状态下不支持x/IX锁
 */
auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  switch (txn->GetIsolationLevel()) {
    // REPEATABLE_READ隔离级别下，SHRINKING是状态下不允许任何锁
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      // READ_COMMITTED隔离级别下，SHRINKING状态下，不允许X/IX/SIX锁
      if (txn->GetState() == TransactionState::SHRINKING) {
        if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
            lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
          return false;
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      // READ_UNCOMMITTED隔离级别下，只支持X和IX锁
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
      break;
    default:
      break;
  }
  return true;
}

/*
 * 判断两个锁是否相容
    Compatibility Matrix
        IS   IX   S   SIX   X
  IS    √    √    √    √    ×
  IX    √    √    ×    ×    ×
  S     √    ×    √    ×    ×
  SIX   √    ×    ×    ×    ×
  X     ×    ×    ×    ×    ×
*/
auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) const -> bool {
  switch (l1) {
    case LockMode::INTENTION_SHARED:
      return l2 != LockMode::EXCLUSIVE;
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
      break;
    case LockMode::SHARED:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED;
      break;
    case LockMode::EXCLUSIVE:
      return false;
      break;
    default:
      return true;
  }
  return true;
}

/*
 * 检测锁能否升级
 * IS -> [S, X, IX, SIX]
 * S -> [X, SIX]
 * IX -> [X, SIX]
 * SIX -> [X]
 */
auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == requested_lock_mode) {
    return true;
  }
  switch (curr_lock_mode) {
    case LockMode::INTENTION_SHARED:
      return true;
      break;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (requested_lock_mode != LockMode::EXCLUSIVE && requested_lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (requested_lock_mode != LockMode::EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::EXCLUSIVE:
      return false;
      break;
    default:
      break;
  }
  return true;
}

/*
 * 加行级锁前保证已经加了相应的表级锁
 */
auto LockManager::EasureTableLockBeforeRowLock(Transaction *txn, LockMode lock_mode, table_oid_t oid) const -> bool {
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableSharedIntentionExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  }
  return true;
}

/*
 * 尝试执行锁请求
 */
auto LockManager::GrantLockIfPossible(const std::shared_ptr<LockRequest> &lock_request,
                                      const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (const auto &lock_request_it : lock_request_queue->request_queue_) {
    if (lock_request_it->granted_) {
      if (!AreLocksCompatible(lock_request_it->lock_mode_, lock_request->lock_mode_)) {
        return false;
      }
    } else if (lock_request_it != lock_request) {
      return false;
    } else {
      return true;
    }
  }
  return true;
}

void LockManager::InsertTableLock(Transaction *txn, table_oid_t oid, LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    default:
      break;
  }
}

void LockManager::DeleteTableLock(Transaction *txn, table_oid_t oid, LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    default:
      break;
  }
}

void LockManager::InsertRowLock(Transaction *txn, table_oid_t oid, RID rid, LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::SHARED:
      if (txn->GetSharedRowLockSet()->count(oid) == 0) {
        (*(txn->GetSharedRowLockSet()))[oid] = std::unordered_set<RID>();
      }
      txn->GetSharedRowLockSet()->at(oid).insert(rid);
      break;
    case LockMode::EXCLUSIVE:
      if (txn->GetExclusiveRowLockSet()->count(oid) == 0) {
        (*(txn->GetExclusiveRowLockSet()))[oid] = std::unordered_set<RID>();
      }
      txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
      break;
    default:
      break;
  }
}

void LockManager::DeleteRowLock(Transaction *txn, table_oid_t oid, RID rid, LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
    default:
      break;
  }
}

auto LockManager::EasureUnlockRowBeforeUnlockTable(Transaction *txn, const table_oid_t &oid) const -> bool {
  if (txn->GetSharedRowLockSet()->count(oid) != 0) {
    if (!txn->GetSharedRowLockSet()->at(oid).empty()) {
      return false;
    }
  }
  if (txn->GetExclusiveRowLockSet()->count(oid) != 0) {
    if (!txn->GetExclusiveRowLockSet()->at(oid).empty()) {
      return false;
    }
  }
  return true;
}

/********************************************************************************************************************/
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto &txn_id_vector = waits_for_[t1];
  if (std::count(txn_id_vector.begin(), txn_id_vector.end(), t2) == 0) {
    txn_id_vector.emplace_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto t2_iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (t2_iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(t2_iter);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // 首先排序
  std::vector<txn_id_t> txn_id_vector;
  txn_id_vector.reserve(waits_for_.size());
  for (auto &iter : waits_for_) {
    std::sort(iter.second.begin(), iter.second.end());
    txn_id_vector.emplace_back(iter.first);
  }

  std::sort(txn_id_vector.begin(), txn_id_vector.end());
  std::stack<txn_id_t> path;
  std::unordered_map<txn_id_t, bool> visited;
  std::function<void(txn_id_t)> dfs = [&](txn_id_t t1) {
    visited[t1] = true;  // 记录访问过
    path.push(t1);
    // 遍历它的所有邻居
    for (txn_id_t t2 : waits_for_[t1]) {
      if (!visited[t2]) {  // 还没有被访问过的邻居
        dfs(t2);
      } else {                        // 该节点已被访问 构成环路
        std::vector<txn_id_t> cycle;  // 记录环路
        *txn_id = t1;
        return;
      }
      if (*txn_id != INVALID_TXN_ID) {
        return;
      }
    }

    visited[t1] = false;
    path.pop();
  };

  *txn_id = INVALID_TXN_ID;
  for (auto txn_id_it : txn_id_vector) {
    dfs(txn_id_it);
    if (*txn_id != INVALID_TXN_ID) {
      break;
    }
  }
  return *txn_id != INVALID_TXN_ID;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &iter : waits_for_) {
    txn_id_t t1 = iter.first;
    for (auto t2 : iter.second) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
//      LOG_INFO("# RunCycleDetection : begin");
      waits_for_.clear();
      waits_for_latch_.lock();
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      // 构建图
      // 遍历每张表的请求txn和占有txn
      for (const auto &iter : table_lock_map_) {
        auto lock_request_queue = iter.second;
        std::vector<txn_id_t> hold_lock_txn;
        std::vector<txn_id_t> block_txn;
        for (const auto &lock_request : lock_request_queue->request_queue_) {
          if (lock_request->granted_) {
            hold_lock_txn.emplace_back(lock_request->txn_id_);
          } else {
            block_txn.emplace_back(lock_request->txn_id_);
          }
        }
        // 添加边
        for (auto t1 : block_txn) {
          for (auto t2 : hold_lock_txn) {
            AddEdge(t1, t2);
          }
        }
      }
      // 遍历每个row的请求txn和占有txn
      for (const auto &iter : row_lock_map_) {
        auto lock_request_queue = iter.second;
        std::vector<txn_id_t> hold_lock_txn;
        std::vector<txn_id_t> block_txn;
        for (const auto &lock_request : lock_request_queue->request_queue_) {
          if (lock_request->granted_) {
            hold_lock_txn.emplace_back(lock_request->txn_id_);
          } else {
            block_txn.emplace_back(lock_request->txn_id_);
          }
        }
        // 添加边
        for (auto t1 : block_txn) {
          for (auto t2 : hold_lock_txn) {
            AddEdge(t1, t2);
          }
        }
      }

      txn_id_t remove_txn_id = INVALID_TXN_ID;
      while (HasCycle(&remove_txn_id)) {
//        LOG_INFO("# RunCycleDetection : find a cycle , remove txn %d", remove_txn_id);
        // 找到一个环
        assert(waits_for_[remove_txn_id].size() == 1);
        // 删除边
        RemoveEdge(remove_txn_id, waits_for_[remove_txn_id][0]);
        // 将remove_txn_id的状态设为ABORTED
        Transaction *remove_txn = txn_manager_->GetTransaction(remove_txn_id);
        remove_txn->SetState(TransactionState::ABORTED);

        // 找到给txn正在申请的锁
        std::shared_ptr<LockRequestQueue> remove_lock_request_queue;
        for (const auto &iter : table_lock_map_) {
          auto lock_request_queue = iter.second;
          for (const auto &lock_request : lock_request_queue->request_queue_) {
            if (lock_request->txn_id_ == remove_txn_id && !lock_request->granted_) {
              remove_lock_request_queue = lock_request_queue;
//              LOG_INFO("# RunCycleDetection : will remove txn %d asking table %d", remove_txn_id, iter.first);
              break;
            }
          }
        }
        for (const auto &iter : row_lock_map_) {
          auto lock_request_queue = iter.second;
          for (const auto &lock_request : lock_request_queue->request_queue_) {
            if (lock_request->txn_id_ == remove_txn_id && !lock_request->granted_) {
              remove_lock_request_queue = lock_request_queue;
//              LOG_INFO("# RunCycleDetection : will remove txn %d asking row <%d %d>", remove_txn_id,
//                       iter.first.GetPageId(), iter.first.GetSlotNum());
              break;
            }
          }
        }

        remove_lock_request_queue->cv_.notify_all();
      }
//      LOG_INFO("# RunCycleDelection : have no cycle");
      // 已经没有环了
      waits_for_latch_.unlock();
      table_lock_map_latch_.unlock();
      row_lock_map_latch_.unlock();
//      LOG_INFO("# RunCycleDetection : done");
    }
  }
}

}  // namespace bustub
