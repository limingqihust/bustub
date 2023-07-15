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
  LOG_INFO("# LockTable : txn_id %d try to lock table %d with %s", txn->GetTransactionId(), oid,
           fmt::format("lock mode {}", lock_mode).c_str());
  // 检查是不是符合隔离级别
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }
  //  LOG_INFO("# LockTable : txn_id %d asking table %d with %s comply with correct isolation", txn->GetTransactionId(),
  //           oid, fmt::format("lock mode {}", lock_mode).c_str());
  // 获取该table的LockRequestQueue
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }

  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for (LockRequest *lock_request : lock_request_queue->request_queue_) {
    // 该txn对该table已经有了一个锁了 尝试升级锁
    // 这个锁一定是获取了的 因为不可能正在请求一个锁还去申请另一个锁
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      //      LOG_INFO("# LockTable : txn_id %d has a lock of table %d ,try to upgrade to %s", txn->GetTransactionId(),
      //      oid,
      //               fmt::format("lock mode {}", lock_mode).c_str());
      return UpgradeLockTable(txn, lock_mode, oid);
    }
  }
  // 该txn对该table不持有锁 新建一个锁请求
  //  LOG_INFO("# LockTable : txn_id %d create a new request to  table %d with %s", txn->GetTransactionId(), oid,
  //           fmt::format("lock mode {}", lock_mode).c_str());
  auto new_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  LOG_INFO("# new : %p", new_lock_request);
  auto iter = lock_request_queue->request_queue_.begin();
  for (; iter != lock_request_queue->request_queue_.end(); iter++) {
    if (!(*iter)->granted_) {
      break;
    }
  }
  lock_request_queue->request_queue_.insert(iter, new_lock_request);
  // 尝试获取锁
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLockIfPossible(new_lock_request, lock_request_queue.get())) {
    lock_request_queue->cv_.wait(lock);
  }
  // 成功获取锁
  LOG_INFO("# LockTable : txn_id %d successfully get lock of table %d with %s", txn->GetTransactionId(), oid,
           fmt::format("lock mode {}", lock_mode).c_str());
  new_lock_request->granted_ = true;
  InsertTableLock(txn, oid, lock_mode);
  lock_request_queue->latch_.unlock();
  lock_request_queue->cv_.notify_all();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  LOG_INFO("# UnlockTable : txn %d unlock table %d", txn->GetTransactionId(), oid);
  table_lock_map_latch_.lock();

  // 没有对该table的锁请求队列
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  // 寻找该txn对该table的锁请求
  for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
       iter++) {
    LockRequest *lock_request = *iter;
    // 找到该txn对该table的锁请求 成功释放 并唤醒其他等待该table的锁的线程
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      DeleteTableLock(txn, oid, lock_request->lock_mode_);
      // 修改该txn的状态
      ChangeTxnState(txn, lock_request->lock_mode_);
      // 从该table的锁请求队列中删除该txn对该锁的请求
      lock_request_queue->request_queue_.erase(iter);
      LOG_INFO("# delete : %p", lock_request);
      delete lock_request;
      lock_request_queue->latch_.unlock();
      lock_request_queue->cv_.notify_all();
      return true;
    }
  }

  // 该事务不对该table持有任何锁
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

/*
 * 行级锁不支持意向锁
 * 加行级锁前需要保证表级锁
 */
auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 行级锁不支持意向锁
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

/*
 * 升级一个表级锁
 */
auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 找到该table的锁请求序列
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  for (LockRequest *lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    // 找到该txn对该table的锁的一个请求
    if (lock_request->lock_mode_ == lock_mode) {  // 如果已获得对该table的同级锁
      lock_request_queue->latch_.unlock();
      return true;
    }

    // 有其他txn对该table有一个锁升级请求 不合法
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      lock_request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      return false;
    }

    // 检查锁升级操作是否合法
    if (!CanLockUpgrade(lock_request->lock_mode_, lock_mode)) {
      lock_request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    // 下面进行锁升级操作
    // 删除旧的锁请求
    lock_request_queue->request_queue_.remove(lock_request);
    DeleteTableLock(txn, oid, lock_request->lock_mode_);
    LOG_INFO("# delete : %p", lock_request);
    delete lock_request;

    // 插入新的锁请求
    auto *new_upgrading_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    LOG_INFO("# new : %p", new_upgrading_lock_request);
    auto insert_pos = lock_request_queue->request_queue_.begin();
    for (; insert_pos != lock_request_queue->request_queue_.end(); insert_pos++) {
      if (!(*insert_pos)->granted_) {
        break;
      }
    }

    lock_request_queue->request_queue_.insert(insert_pos, new_upgrading_lock_request);
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
    while (!GrantLockIfPossible(new_upgrading_lock_request, lock_request_queue.get())) {
      lock_request_queue->cv_.wait(lock);
    }

    // 完成升级锁请求
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    new_upgrading_lock_request->granted_ = true;
    InsertTableLock(txn, oid, new_upgrading_lock_request->lock_mode_);
    lock_request_queue->latch_.unlock();
    lock_request_queue->cv_.notify_all();
    return true;
  }
  return false;
}

/*
 * 升级一个行级锁
 */
auto LockManager::UpgradeLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
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
    default:
      break;
  }
  return true;
}

/*
 * 加行级锁前保证已经加了相应的表级锁
 */
auto LockManager::CanTxnTakeRowLock(Transaction *txn, LockMode lock_mode, table_oid_t oid) const -> bool {
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableSharedIntentionExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableExclusiveLocked(oid)) {
      return false;
    }
  }
  if (lock_mode == LockMode::SHARED) {
    if (!txn->IsTableSharedIntentionExclusiveLocked(oid) && !txn->IsTableSharedLocked(oid) &&
        !txn->IsTableIntentionSharedLocked(oid)) {
      return false;
    }
  }
  return true;
}

/*
 * 尝试执行锁请求
 */
auto LockManager::GrantLockIfPossible(LockRequest *lock_request, LockRequestQueue *lock_request_queue) -> bool {
  for (auto lock_request_it : lock_request_queue->request_queue_) {
    if (lock_request_it == lock_request) {
      lock_request->granted_ = true;
      return true;
    }
    if (lock_request_it->granted_) {
      if (!AreLocksCompatible(lock_request_it->lock_mode_, lock_request->lock_mode_)) {
        return false;
      }
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

/*
 * 当释放锁后 检查是不是需要修改txn的状态有GROWING到SHRINKING
 */
void LockManager::ChangeTxnState(Transaction *txn, LockMode lock_mode) {
  if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
       (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE)) ||
      (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mode == LockMode::EXCLUSIVE) ||
      (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::EXCLUSIVE)) {
    if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
}

/********************************************************************************************************************/
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
