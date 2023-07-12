//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  index_info_ = exec_ctx->GetCatalog()->GetIndex(plan->index_oid_);
  table_info_ = exec_ctx->GetCatalog()->GetTable(index_info_->table_name_);
  tree_index_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  iter_ = tree_index_->GetBeginIterator();
}

void IndexScanExecutor::Init() {
  iter_ = tree_index_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &bpm = exec_ctx_->GetBufferPoolManager();
  while (true) {
    if (!iter_.IsEnd()) {
      *rid = (*iter_).second;
      ++iter_;
      auto table_page = reinterpret_cast<TablePage *>(bpm->FetchPage(rid->GetPageId())->GetData());
      *tuple = table_page->GetTuple(*rid).second;
      auto tuplemeta = table_page->GetTuple(*rid).first;
      bpm->UnpinPage(rid->GetPageId(), false);
      if (!tuplemeta.is_deleted_) {
        return true;
      }
    } else {
      return false;
    }
  }
}

}  // namespace bustub
