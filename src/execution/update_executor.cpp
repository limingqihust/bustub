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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);  // 需要更新的table
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int update_cnt = 0;
  Tuple old_tuple;
  RID old_rid;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    // 删除旧tuple
    TupleMeta old_tuple_meta = table_info_->table_->GetTupleMeta(old_rid);
    old_tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(old_tuple_meta, old_rid);

    // 计算新tuple
    std::vector<Value> value_of_new_tuple;
    for (const auto &expression : plan_->target_expressions_) {
      value_of_new_tuple.emplace_back(expression->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }

    Tuple new_tuple(value_of_new_tuple, &child_executor_->GetOutputSchema());
    TupleMeta new_tuple_meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};

    // 插入到table中
    RID new_rid = (table_info_->table_->InsertTuple(new_tuple_meta, new_tuple, exec_ctx_->GetLockManager(),
                                                    exec_ctx_->GetTransaction(), table_info_->oid_))
                      .value();
    update_cnt++;

    // 更新索引
    std::vector<IndexInfo *> index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (const auto &index_info : index_infos) {
      // 删除旧索引
      auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                            index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());

      // 插入新索引
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                            index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
    }
  }
  is_end_ = true;
  std::vector<Value> value_of_return_tuple;
  value_of_return_tuple.emplace_back(TypeId::INTEGER, update_cnt);
  *tuple = Tuple(value_of_return_tuple, &GetOutputSchema());
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
