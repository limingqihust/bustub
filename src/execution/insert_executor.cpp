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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan->table_oid_);
  table_heap_ = table_info_->table_.get();
}

void InsertExecutor::Init() {
  child_executor_->Init();
  // 加表级锁 X锁
  try {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                plan_->TableOid())) {
      throw ExecutionException("[InsertExecutor] lock table failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("[InsertExecutor] lock table failed");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int insert_cnt = 0;
  Tuple new_tuple;
  RID new_rid;
  while (child_executor_->Next(&new_tuple, &new_rid)) {
    // 插入新tuple
    TupleMeta tuple_meta_to_insert = {INVALID_TXN_ID, INVALID_TXN_ID, false};
    new_rid = (table_heap_->InsertTuple(tuple_meta_to_insert, new_tuple, exec_ctx_->GetLockManager(),
                                        exec_ctx_->GetTransaction(), table_info_->oid_))
                  .value();
    // 更新write set
    exec_ctx_->GetTransaction()->AppendTableWriteRecord({plan_->TableOid(), new_rid, table_heap_});
    insert_cnt++;

    // 更新索引
    std::vector<IndexInfo *> index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (const auto &index_info : index_infos) {
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                            index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
      // 更新write set
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(
          {new_rid, plan_->TableOid(), WType::INSERT, new_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()});
    }
  }
  is_end_ = true;
  std::vector<Value> value_of_return_tuple;
  value_of_return_tuple.emplace_back(TypeId::INTEGER, insert_cnt);
  *tuple = Tuple(value_of_return_tuple, &GetOutputSchema());
  *rid = tuple->GetRid();
  return true;
}
}  // namespace bustub
