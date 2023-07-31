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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_heap_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)->table_.get()),
      table_iterator_ptr_(std::make_shared<TableIterator>(table_heap_->MakeEagerIterator())),
      transaction_(exec_ctx->GetTransaction()),
      lock_mgr_(exec_ctx->GetLockManager()),
      txn_mgr_(exec_ctx->GetTransactionManager()) {}

void SeqScanExecutor::Init() {
  table_iterator_ptr_ = std::make_shared<TableIterator>(table_heap_->MakeEagerIterator());
  try {
    LOG_INFO("# SeqScanExecutor : txn %d try to lock table %d with %s",transaction_->GetTransactionId(),
             plan_->GetTableOid(), fmt::format("IsolationLevel : {}", transaction_->GetIsolationLevel()).c_str());
    if (!LockTable(transaction_, lock_mgr_, plan_->GetTableOid())) {
      throw ExecutionException("[SeqScanExecutor] get table lock failed");
    }
    LOG_INFO("# SeqScanExecutor : txn %d successfully lock table %d",
             transaction_->GetTransactionId(), plan_->GetTableOid());
  } catch (TransactionAbortException &e) {
    throw ExecutionException("[SeqScanExecutor] get table lock failed");
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    try {
      LOG_INFO("# SeqScanExecutor : txn %d try to lock table %d row",
               transaction_->GetTransactionId(), plan_->GetTableOid());
      if (!LockRow(transaction_, lock_mgr_, plan_->GetTableOid(), table_iterator_ptr_->GetRID())) {  // 对row加锁
        throw ExecutionException("[SeqScanExecutor] get row table failed");
      }
      LOG_INFO("# SeqScanExecutor : txn %d successful lock table %d row",
               transaction_->GetTransactionId(), plan_->GetTableOid());
    } catch (TransactionAbortException &e) {
      throw ExecutionException("[SeqScanExecutor] get row table failed");
    }
    if (!table_iterator_ptr_->IsEnd()) {
      auto tuplemeta = table_iterator_ptr_->GetTuple().first;
      if (!tuplemeta.is_deleted_) {
        *rid = table_iterator_ptr_->GetRID();
        *tuple = table_iterator_ptr_->GetTuple().second;
        UnlockRow(transaction_, lock_mgr_, plan_->GetTableOid(), table_iterator_ptr_->GetRID());  // 对row解锁
        LOG_INFO("# SeqScanExecutor : txn %d unlock table %d row",
                 transaction_->GetTransactionId(), plan_->GetTableOid());
        ++(*table_iterator_ptr_);
        return true;
      }
      lock_mgr_->UnlockRow(transaction_, plan_->GetTableOid(), table_iterator_ptr_->GetRID(), true);  // 对row强制解锁
      LOG_INFO("# SeqScanExecutor : txn %d unlock table %d row",transaction_->GetTransactionId(), plan_->GetTableOid());
      ++(*table_iterator_ptr_);
    } else {
      return false;
    }
  }
}

/*
 * 读操作
 * REPEATABLE_READ 加S锁直到commit或者abort
 * 对于READ_COMMITTED 加S锁 可以立即释放
 * 对于READ_UNCOMMITTED 不需要加任何S锁
 */
auto SeqScanExecutor::LockTable(Transaction *txn, LockManager *lock_mgr, const table_oid_t oid) -> bool {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
    case IsolationLevel::READ_COMMITTED:
      return lock_mgr->LockTable(txn, LockManager::LockMode::SHARED, oid);
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      break;
  }
  return true;
}

auto SeqScanExecutor::UnlockTable(Transaction *txn, LockManager *lock_mgr, const table_oid_t oid) -> bool {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
    case IsolationLevel::READ_COMMITTED:
      return lock_mgr->UnlockTable(txn, oid);
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      break;
  }
  return true;
}

auto SeqScanExecutor::LockRow(Transaction *txn, LockManager *lock_mgr, const table_oid_t oid, const RID rid) -> bool {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
    case IsolationLevel::READ_COMMITTED:
      return lock_mgr->LockRow(txn, LockManager::LockMode::SHARED, oid, rid);
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      break;
  }
  return true;
}

auto SeqScanExecutor::UnlockRow(Transaction *txn, LockManager *lock_mgr, const table_oid_t oid, const RID rid) -> bool {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
    case IsolationLevel::READ_COMMITTED:
      return lock_mgr->UnlockRow(txn, oid, rid);
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      break;
  }
  return true;
}

}  // namespace bustub
