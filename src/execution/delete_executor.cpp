//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);  // 需要更新的table
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int delete_cnt = 0;
  Tuple old_tuple;
  RID old_rid;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    // 删除旧tuple
    TupleMeta old_tuple_meta = table_info_->table_->GetTupleMeta(old_rid);
    old_tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(old_tuple_meta, old_rid);
    delete_cnt++;

    // 更新索引
    std::vector<IndexInfo *> index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (const auto &index_info : index_infos) {
      // 删除旧索引
      auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                            index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());
      // 更新write set
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(
          {old_rid, plan_->TableOid(), WType::DELETE, old_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()});
    }
  }
  is_end_ = true;
  std::vector<Value> value_of_return_tuple;
  value_of_return_tuple.emplace_back(TypeId::INTEGER, delete_cnt);
  *tuple = Tuple(value_of_return_tuple, &GetOutputSchema());
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
