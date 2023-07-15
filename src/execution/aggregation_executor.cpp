//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      hash_(plan->GetAggregates(), plan->GetAggregateTypes()),
      hash_iter_(hash_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  hash_.Clear();
  Tuple child_tuple;
  RID child_rid;
  while (child_->Next(&child_tuple, &child_rid)) {
    auto agg_key = MakeAggregateKey(&child_tuple);
    auto agg_value = MakeAggregateValue(&child_tuple);
    hash_.InsertCombine(agg_key, agg_value);
  }
  if (hash_.Empty() && plan_->GetGroupBys().empty()) {
    hash_.MakeEmpty({});
  }
  hash_iter_ = hash_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (hash_iter_ != hash_.End()) {
    std::vector<Value> values;
    if (!plan_->GetGroupBys().empty()) {
      values = hash_iter_.Key().group_bys_;
    }
    values.insert(values.end(), hash_iter_.Val().aggregates_.begin(), hash_iter_.Val().aggregates_.end());
    *tuple = Tuple(std::move(values), &plan_->OutputSchema());
    ++hash_iter_;
    return true;
  }
  return false;

  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
