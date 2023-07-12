//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child_executor)){
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_->Init();
  index_info_=exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_=exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  Tuple right_tuple;
  RID left_rid;
  RID right_rid;
  std::vector<Value> join_value;
  while(child_->Next(&left_tuple,&left_rid))
  {

    auto left_key=plan_->KeyPredicate()->Evaluate(&left_tuple,child_->GetOutputSchema());
    std::vector<RID> right_rids;
    auto tree_index=dynamic_cast<BPlusTreeIndexForTwoIntegerColumn*>(index_info_->index_.get());
    tree_index->ScanKey(Tuple({left_key},&(index_info_->key_schema_)),&right_rids,exec_ctx_->GetTransaction());
    if(!right_rids.empty()) {
      right_tuple=table_info_->table_->GetTuple(right_rids[0]).second;
      for(uint32_t i=0;i<child_->GetOutputSchema().GetColumnCount();i++) {            // 外表
        join_value.emplace_back(left_tuple.GetValue(&child_->GetOutputSchema(),i));
      }
      for(uint32_t i=0;i<GetOutputSchema().GetColumnCount();i++) {                    // 内表
        join_value.emplace_back(right_tuple.GetValue(&plan_->InnerTableSchema(),i));
      }
      *tuple=Tuple(join_value,&GetOutputSchema());
      return true;
    }
  }


  return false;
}

}  // namespace bustub
