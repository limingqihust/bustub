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
      table_iterator_(table_heap_->MakeIterator()) {}

void SeqScanExecutor::Init() {
  table_iterator_ = table_heap_->MakeIterator();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!table_iterator_.IsEnd()) {
      auto tuplemeta = table_iterator_.GetTuple().first;
      if (!tuplemeta.is_deleted_) {
        *rid = table_iterator_.GetRID();
        *tuple = table_iterator_.GetTuple().second;
        ++table_iterator_;
        return true;
      }
      ++table_iterator_;
    } else {
      return false;
    }
  }
}

}  // namespace bustub
