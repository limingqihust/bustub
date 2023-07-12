#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
//  std::vector<AbstractPlanNodeRef> children;
//  for(const auto& child : plan->GetChildren()) {
//    children.emplace_back(OptimizeNLJAsHashJoin(child));
//  }
//  auto optimized_plan=plan->CloneWithChildren(std::move(children));
//  if(optimized_plan->GetType()==PlanType::NestedLoopJoin) {
//    const auto& nlj_plan=dynamic_cast<const NestedLoopJoinPlanNode&>(*optimized_plan);
//    BUSTUB_ENSURE(nlj_plan.children_.size()==2,"NLJ should have exactly 2 children.");
//    const auto expr=dynamic_cast<const ComparisonExpression*>(nlj_plan.Predicate().get());
//    assert(expr!=nullptr);
//    if(expr->comp_type_==ComparisonType::Equal) {
//      const auto left_expr=dynamic_cast<const ComparisonExpression*>(expr->children_[0].get());
//      const auto right_expr=dynamic_cast<const ComparisonExpression*>(expr->children_[1].get());
//      assert(left_expr!=nullptr);
//      assert(right_expr!=nullptr);
//      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),nlj_plan.GetRightPlan(),
//                                                std::vector<AbstractExpressionRef>({left_expr}),
//                                                std::move(right_expr_tuple_0), nlj_plan.GetJoinType());
//
//  }
//  return optimized_plan;
  return plan;
}

}  // namespace bustub
