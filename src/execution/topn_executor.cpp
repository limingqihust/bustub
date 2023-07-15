#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), limit_(plan->GetN()) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  heap_.clear();
  auto cmp = [order_bys = plan_->GetOrderBy(), schema = plan_->OutputSchema()](const Tuple left_tuple,
                                                                               const Tuple right_tuple) -> bool {
    for (const auto &order_by : order_bys) {
      OrderByType order_type = order_by.first;
      AbstractExpressionRef expr = order_by.second;
      Value left_key = expr->Evaluate(&left_tuple, schema);
      Value right_key = expr->Evaluate(&right_tuple, schema);
      if (left_key.CompareEquals(right_key) == CmpBool::CmpTrue) {
        continue;
      }
      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {  // 默认升序
        return left_key.CompareLessThan(right_key) == CmpBool::CmpTrue;
      }
      return left_key.CompareGreaterThan(right_key) == CmpBool::CmpTrue;  // 降序
    }
    return true;
  };

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    if (heap_.size() == limit_) {
      if (cmp(tuple, heap_[0])) {
        std::pop_heap(heap_.begin(), heap_.end(), cmp);
        heap_[limit_ - 1] = tuple;
        std::push_heap(heap_.begin(), heap_.end(), cmp);  // 调整位置
      }
    } else {
      heap_.emplace_back(tuple);
      std::push_heap(heap_.begin(), heap_.end(), cmp);
    }
  }
  std::sort_heap(heap_.begin(), heap_.end(), cmp);
  iter_ = heap_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ != heap_.end()) {
    *tuple = *iter_;
    ++iter_;
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_.size(); };

}  // namespace bustub
