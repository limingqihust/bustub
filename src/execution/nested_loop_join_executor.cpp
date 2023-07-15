//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_executor)),
      right_child_(std::move(right_executor)),
      left_schema_(plan->GetLeftPlan()->OutputSchema()),
      right_schema_(plan->GetRightPlan()->OutputSchema()) {
  left_child_->Init();
  right_child_->Init();
  left_child_->Next(&left_tuple_, &left_rid_);
  right_empty_ = !right_child_->Next(&right_tuple_, &right_rid_);
  done_ = false;
  left_match_ = false;
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  left_child_->Next(&left_tuple_, &left_rid_);
  right_empty_ = !right_child_->Next(&right_tuple_, &right_rid_);
  done_ = false;
  left_match_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  if (right_empty_ && plan_->GetJoinType() == JoinType::LEFT) {
    right_child_->Init();
    std::vector<Value> join_value;
    for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
      join_value.emplace_back(left_tuple_.GetValue(&left_schema_, i));
    }
    for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
      join_value.emplace_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
    }
    *tuple = Tuple(join_value, &GetOutputSchema());
    *rid = tuple->GetRid();
    done_ = !left_child_->Next(&left_tuple_, &left_rid_);
    return true;
  }
  do {
    do {
      auto join_flag = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_schema_, &right_tuple_, right_schema_);
      if (!join_flag.IsNull() && join_flag.GetAs<bool>()) {
        left_match_ = true;
        std::vector<Value> join_value;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          join_value.emplace_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          join_value.emplace_back(right_tuple_.GetValue(&right_schema_, i));
        }
        *tuple = Tuple(join_value, &GetOutputSchema());
        *rid = tuple->GetRid();
        if (!right_child_->Next(&right_tuple_, &right_rid_)) {
          right_child_->Init();
          right_child_->Next(&right_tuple_, &right_rid_);
          if (!left_child_->Next(&left_tuple_, &left_rid_)) {
            done_ = true;
          }
          left_match_ = false;
        }
        return true;
      }
    } while (right_child_->Next(&right_tuple_, &right_rid_));

    if (plan_->GetJoinType() == JoinType::LEFT && !left_match_) {
      std::vector<Value> join_value;
      for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
        join_value.emplace_back(left_tuple_.GetValue(&left_schema_, i));
      }
      for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
        join_value.emplace_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
      }
      *tuple = Tuple(join_value, &GetOutputSchema());
      *rid = tuple->GetRid();
      right_child_->Init();
      right_child_->Next(&right_tuple_, &right_rid_);
      left_match_ = false;
      if (!left_child_->Next(&left_tuple_, &left_rid_)) {
        done_ = true;
      }
      return true;
    }

    right_child_->Init();
    right_child_->Next(&right_tuple_, &right_rid_);
    left_match_ = false;
  } while (left_child_->Next(&left_tuple_, &right_rid_));

  return false;

  //  while(left_child_->Next(&left_tuple,&left_rid)) {
  //    right_child_->Init();
  //
  //    while(right_child_->Next(&right_tuple,&right_rid)) {
  //      auto join_flag=plan_->Predicate()->EvaluateJoin(&left_tuple,left_schema_,&right_tuple,right_schema_);
  //      if(!join_flag.IsNull() && join_flag.GetAs<bool>()) {
  //        std::vector<Value> join_value;
  //        for(uint32_t i=0;i<left_schema_.GetColumnCount();i++) {
  //          join_value.emplace_back(left_tuple.GetValue(&left_schema_,i));
  //        }
  //        for(uint32_t i=0;i<right_schema_.GetColumnCount();i++) {
  //          join_value.emplace_back(right_tuple.GetValue(&right_schema_,i));
  //        }
  //
  //        *tuple=Tuple(join_value,&GetOutputSchema());
  //        *rid=tuple->GetRid();
  //
  //        return true;
  //      }
  //    }
  //    if(plan_->GetJoinType()==JoinType::LEFT) {
  //      std::vector<Value> join_value;
  //      for(uint32_t i=0;i<left_schema_.GetColumnCount();i++) {
  //        join_value.emplace_back(left_tuple.GetValue(&left_schema_,i));
  //      }
  //      for(uint32_t i=0;i<right_schema_.GetColumnCount();i++) {
  //        join_value.emplace_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
  //      }
  //      *tuple=Tuple(join_value,&GetOutputSchema());
  //      *rid=tuple->GetRid();
  //      return true;
  //    }
  //  }
}

}  // namespace bustub
