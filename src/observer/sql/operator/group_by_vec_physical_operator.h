/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/expr/aggregate_hash_table.h"
#include "sql/operator/physical_operator.h"

/**
 * @brief Group By 物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class GroupByVecPhysicalOperator : public PhysicalOperator
{
public:
  GroupByVecPhysicalOperator(
      std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions)
      {
        group_expressions = std::move(group_by_exprs);
        aggre_expressions = expressions;
        aggre_value_expressions.reserve(aggre_expressions.size());
        for(auto expr : aggre_expressions)
        {
          auto* aggr_expr = static_cast<AggregateExpr*>(expr);
          Expression* child = aggr_expr->child().get();
          ASSERT(child != nullptr, "aggregation expression must have a child expression");
          aggre_value_expressions.emplace_back(child);
        } 
        ht_ = std::make_unique<StandardAggregateHashTable>(expressions);
        scanner = std::make_unique<StandardAggregateHashTable::Scanner>(ht_.get());
      };

  virtual ~GroupByVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUP_BY_VEC; }

  RC open(Trx *trx) override {
    ASSERT(children_.size()==1,"vectorized group by operator only support one child, but got %d", children_.size()); 
    PhysicalOperator& child = *children_[0];
    RC rc = child.open(trx);
    if(OB_FAIL(rc))
    {
      LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
      return rc;
    }
    while(OB_SUCC(rc =child.next(chunk_)))
    {
      int col_id = 0;
      Chunk group_chunk,aggr_chunk;
      for(auto& group_expr:group_expressions)
      {
        Column col;
        group_expr->get_column(chunk_,col);
        group_chunk.add_column(make_unique<Column>(col.attr_type(),col.attr_len()),col_id);
        group_chunk.column(col_id).append(col.data(),col.count());
        col_id++;
      }
      col_id=0;
      for(auto & aggr_expr : aggre_value_expressions)
      {
        Column col;
        aggr_expr->get_column(chunk_,col);
        aggr_chunk.add_column(make_unique<Column>(col.attr_type(),col.attr_len()),col_id);
        aggr_chunk.column(col_id).append(col.data(),col.count());
        col_id++;
      }
      rc=ht_->add_chunk(group_chunk,aggr_chunk);
      if (OB_FAIL(rc)) {
        LOG_INFO("failed to add chunks. rc=%s", strrc(rc));
        return rc;
      }
      
    }
    scanner->open_scan();
    if(rc==RC::RECORD_EOF)
    {
      rc=RC::SUCCESS;
    } 
    emit = false;
    return rc;
  }
  RC next(Chunk &chunk) override { 
    if(emit)
    return RC::RECORD_EOF;
    output_chunk.reset_data();
    int col_id=0;
    for(auto& group_expr: group_expressions)
    {
      output_chunk.add_column(make_unique<Column>(group_expr->value_type(),group_expr->value_length()),col_id);
      col_id++;
    }
    for(auto& val_expr:aggre_value_expressions)
    {
      output_chunk.add_column(make_unique<Column>(val_expr->value_type(),val_expr->value_length()),col_id);
      col_id++;
    }
    RC rc = scanner->next(output_chunk);
    chunk.reference(output_chunk);
    emit = true;
    return rc;
    }
  RC close() override {
    RC rc=children_[0]->close();
    return rc;
   }

private:
  vector<Expression*> aggre_expressions;
  vector<Expression *> aggre_value_expressions;
  vector<std::unique_ptr<Expression>> group_expressions;
  Chunk chunk_,output_chunk;
  bool emit = false;
  std::unique_ptr<StandardAggregateHashTable> ht_;
  std::unique_ptr<StandardAggregateHashTable::Scanner> scanner;

};