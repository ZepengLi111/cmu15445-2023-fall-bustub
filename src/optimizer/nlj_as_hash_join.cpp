#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
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
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    auto *expr_logic = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get());
    auto *last_expr_logic = expr_logic;
    std::vector<const ComparisonExpression *> comp_exprs;
    std::vector<AbstractExpressionRef> left_exprs;
    std::vector<AbstractExpressionRef> right_exprs;
    while (expr_logic != nullptr && expr_logic->logic_type_ == LogicType::And) {
      auto *expr_comp = dynamic_cast<const ComparisonExpression *>(expr_logic->children_[1].get());
      comp_exprs.push_back(expr_comp);
      last_expr_logic = expr_logic;
      expr_logic = dynamic_cast<const LogicExpression *>(expr_logic->children_[0].get());
    }
    if (comp_exprs.empty()) {
      if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
        comp_exprs.push_back(expr);
      }
    } else {
      if (last_expr_logic != nullptr && last_expr_logic->children_[0] != nullptr &&
          last_expr_logic->children_[1] != nullptr) {
        comp_exprs.push_back(dynamic_cast<const ComparisonExpression *>(last_expr_logic->children_[0].get()));
        comp_exprs.push_back(dynamic_cast<const ComparisonExpression *>(last_expr_logic->children_[1].get()));
      }
    }

    for (auto expr_cmp : comp_exprs) {
      if (expr_cmp->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr_cmp->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr_cmp->children_[1].get());
              right_expr != nullptr) {
            if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
              auto left_expr_tuple = std::make_shared<ColumnValueExpression>(
                  left_expr->GetTupleIdx(), left_expr->GetColIdx(), left_expr->GetReturnType());
              left_exprs.push_back(static_cast<AbstractExpressionRef>(left_expr_tuple));
              auto right_expr_tuple = std::make_shared<ColumnValueExpression>(
                  right_expr->GetTupleIdx(), right_expr->GetColIdx(), right_expr->GetReturnType());
              right_exprs.push_back(static_cast<AbstractExpressionRef>(right_expr_tuple));
            } else {
              auto right_expr_tuple = std::make_shared<ColumnValueExpression>(
                  left_expr->GetTupleIdx(), left_expr->GetColIdx(), left_expr->GetReturnType());
              auto left_expr_tuple = std::make_shared<ColumnValueExpression>(
                  right_expr->GetTupleIdx(), right_expr->GetColIdx(), right_expr->GetReturnType());
              left_exprs.push_back(left_expr_tuple);
              right_exprs.push_back(right_expr_tuple);
            }
          }
        }
      }
    }
    if (!left_exprs.empty()) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), left_exprs, right_exprs,
                                                nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
