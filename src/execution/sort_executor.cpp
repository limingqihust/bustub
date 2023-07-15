#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  result_tuple_.clear();
  while (child_executor_->Next(&tuple, &rid)) {
    result_tuple_.emplace_back(tuple);
  }

  std::sort(
      result_tuple_.begin(), result_tuple_.end(),  // lamda 表达式, 匿名函数
      [order_bys = plan_->GetOrderBy(), schema = GetOutputSchema()](const Tuple &tupleA, const Tuple &tupleB) {
        for (const auto &order_by : order_bys) {
          if (order_by.second->Evaluate(&tupleA, schema).CompareEquals(order_by.second->Evaluate(&tupleB, schema)) ==
              CmpBool::CmpTrue) {
            continue;
          }
          switch (order_by.first) {
            case OrderByType::INVALID:
            case OrderByType::DEFAULT:
            case OrderByType::ASC:  // 升序
              return (order_by.second->Evaluate(&tupleA, schema))
                         .CompareLessThan((order_by.second->Evaluate(&tupleB, schema))) == CmpBool::CmpTrue;
              break;
            case OrderByType::DESC:  // 降序
              return (order_by.second->Evaluate(&tupleA, schema))
                         .CompareGreaterThan((order_by.second->Evaluate(&tupleB, schema))) == CmpBool::CmpTrue;
              break;
            default:
              break;
          }
        }
        return false;
      });
  iter_ = result_tuple_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ != result_tuple_.end()) {
    *tuple = *iter_;
    *rid = tuple->GetRid();
    ++iter_;
    return true;
  }

  return false;
}

}  // namespace bustub
