//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  std::shared_ptr<std::deque<IndexWriteRecord>> index_write_set = txn->GetIndexWriteSet();
  std::shared_ptr<std::deque<TableWriteRecord>> table_write_set = txn->GetWriteSet();
  // 恢复修改的索引
  while (!index_write_set->empty()) {
    IndexWriteRecord index_write_record = index_write_set->back();
    index_write_set->pop_back();
    if (index_write_record.wtype_ == WType::INSERT) {
      index_write_record.catalog_->GetIndex(index_write_record.index_oid_)
          ->index_->DeleteEntry(index_write_record.tuple_, index_write_record.rid_, txn);
    } else {
      index_write_record.catalog_->GetIndex(index_write_record.index_oid_)
          ->index_->InsertEntry(index_write_record.tuple_, index_write_record.rid_, txn);
    }
  }
  // 恢复修改的表
  while (!table_write_set->empty()) {
    TableWriteRecord table_write_record = table_write_set->back();
    table_write_set->pop_back();
    TupleMeta old_tuple_meta = table_write_record.table_heap_->GetTupleMeta(table_write_record.rid_);
    old_tuple_meta.is_deleted_ = true;
    table_write_record.table_heap_->UpdateTupleMeta(old_tuple_meta, table_write_record.rid_);
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
