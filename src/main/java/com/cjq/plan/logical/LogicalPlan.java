package com.cjq.plan.logical;

import java.util.function.BiFunction;

public class LogicalPlan extends QueryPlan<LogicalPlan> {

  private LogicalPlan plan;

  public <C> LogicalPlan optionalMap(C ctx, BiFunction<C, LogicalPlan, LogicalPlan> f) {
    if (ctx != null)
      return f.apply(ctx, plan);
    return plan;
  }
}
