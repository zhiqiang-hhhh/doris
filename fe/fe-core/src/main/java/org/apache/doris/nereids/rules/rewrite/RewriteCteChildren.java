// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * rewrite CteAnchor consumer side and producer side recursively， all CteAnchor must at top of the plan
 */
@DependsRules({PullUpCteAnchor.class, CTEInline.class})
public class RewriteCteChildren extends DefaultPlanRewriter<CascadesContext> implements CustomRewriter {

    private final List<RewriteJob> jobs;
    private final boolean runCboRules;

    public RewriteCteChildren(List<RewriteJob> jobs, boolean runCboRules) {
        this.jobs = jobs;
        this.runCboRules = runCboRules;
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, jobContext.getCascadesContext());
    }

    @Override
    public Plan visit(Plan plan, CascadesContext context) {
        Rewriter.getCteChildrenRewriter(context, jobs, runCboRules).execute();
        return context.getRewritePlan();
    }

    @Override
    public Plan visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
            CascadesContext cascadesContext) {
        LogicalPlan outer;
        if (cascadesContext.getStatementContext().getRewrittenCteConsumer().containsKey(cteAnchor.getCteId())) {
            outer = cascadesContext.getStatementContext().getRewrittenCteConsumer().get(cteAnchor.getCteId());
        } else {
            CascadesContext outerCascadesCtx = CascadesContext.newSubtreeContext(
                    Optional.empty(), cascadesContext, cteAnchor.child(1),
                    cascadesContext.getCurrentJobContext().getRequiredProperties());
            AtomicReference<LogicalPlan> outerResult = new AtomicReference<>();
            StatsDerive statsDerive = new StatsDerive(false);
            cteAnchor.child(0).accept(statsDerive, new StatsDerive.DeriveContext());
            outerCascadesCtx.withPlanProcess(cascadesContext.showPlanProcess(), () -> {
                outerResult.set((LogicalPlan) cteAnchor.child(1).accept(this, outerCascadesCtx));
            });
            outer = outerResult.get();
            cascadesContext.addPlanProcesses(outerCascadesCtx.getPlanProcesses());
            cascadesContext.getStatementContext().getRewrittenCteConsumer().put(cteAnchor.getCteId(), outer);
        }
        Set<LogicalCTEConsumer> cteConsumers = Sets.newHashSet();
        outer.foreach(p -> {
            if (p instanceof LogicalCTEConsumer) {
                LogicalCTEConsumer logicalCTEConsumer = (LogicalCTEConsumer) p;
                if (logicalCTEConsumer.getCteId().equals(cteAnchor.getCteId())) {
                    cteConsumers.add(logicalCTEConsumer);
                }
            }
            return false;
        });
        cascadesContext.getCteIdToConsumers().put(cteAnchor.getCteId(), cteConsumers);
        if (cteConsumers.isEmpty()) {
            return outer;
        }
        Plan producer = cteAnchor.child(0).accept(this, cascadesContext);
        return cteAnchor.withChildren(producer, outer);
    }

    @Override
    public Plan visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer,
            CascadesContext cascadesContext) {
        LogicalPlan child;
        if (cascadesContext.getStatementContext().getRewrittenCteProducer().containsKey(cteProducer.getCteId())) {
            child = cascadesContext.getStatementContext().getRewrittenCteProducer().get(cteProducer.getCteId());
        } else {
            child = (LogicalPlan) cteProducer.child();
            child = tryToConstructFilter(cascadesContext, cteProducer.getCteId(), child);
            Set<Slot> producerOutputs = cascadesContext.getStatementContext()
                    .getCteIdToOutputIds().get(cteProducer.getCteId());
            if (producerOutputs.size() < child.getOutput().size()) {
                ImmutableList.Builder<NamedExpression> projectsBuilder
                        = ImmutableList.builderWithExpectedSize(producerOutputs.size());
                for (Slot slot : child.getOutput()) {
                    if (producerOutputs.contains(slot)) {
                        projectsBuilder.add(slot);
                    }
                }
                child = new LogicalProject<>(projectsBuilder.build(), child);
                child = pushPlanUnderAnchor(child);
            }
            CascadesContext rewrittenCtx = CascadesContext.newSubtreeContext(
                    Optional.of(cteProducer.getCteId()), cascadesContext, child, PhysicalProperties.ANY);
            AtomicReference<LogicalPlan> result = new AtomicReference<>(child);
            rewrittenCtx.withPlanProcess(cascadesContext.showPlanProcess(), () -> {
                result.set((LogicalPlan) result.get().accept(this, rewrittenCtx));
            });
            child = result.get();
            cascadesContext.addPlanProcesses(rewrittenCtx.getPlanProcesses());
            cascadesContext.getStatementContext().getRewrittenCteProducer().put(cteProducer.getCteId(), child);
        }
        return cteProducer.withChildren(child);
    }

    private LogicalPlan pushPlanUnderAnchor(LogicalPlan plan) {
        if (plan.child(0) instanceof LogicalCTEAnchor) {
            LogicalPlan child = (LogicalPlan) plan.withChildren(plan.child(0).child(1));
            return (LogicalPlan) plan.child(0).withChildren(
                    plan.child(0).child(0), pushPlanUnderAnchor(child));
        }
        return plan;
    }

    /*
     * An expression can only be pushed down if it has filter expressions on all consumers that reference the slot.
     * For example, let's assume a producer has two consumers, consumer1 and consumer2:
     *
     * filter(a > 5 and b < 1) -> consumer1
     * filter(a < 8) -> consumer2
     *
     * In this case, the only expression that can be pushed down to the producer is filter(a > 5 or a < 8).
     */
    private LogicalPlan tryToConstructFilter(CascadesContext cascadesContext, CTEId cteId, LogicalPlan child) {
        Set<RelationId> consumerIds = cascadesContext.getCteIdToConsumers().get(cteId).stream()
                .map(LogicalCTEConsumer::getRelationId)
                .collect(Collectors.toSet());
        List<Set<Expression>> filtersAboveEachConsumer = cascadesContext.getConsumerIdToFilters().entrySet().stream()
                .filter(kv -> consumerIds.contains(kv.getKey()))
                .map(Entry::getValue)
                .collect(Collectors.toList());
        Set<Expression> someone = filtersAboveEachConsumer.stream().findFirst().orElse(null);
        if (someone == null) {
            return child;
        }
        int filterSize = cascadesContext.getCteIdToConsumers().get(cteId).size();
        Set<Expression> conjuncts = new HashSet<>();
        for (Expression f : someone) {
            int matchCount = 0;
            Set<SlotReference> slots = f.collect(e -> e instanceof SlotReference);
            Set<Expression> mightBeJoined = new HashSet<>();
            for (Set<Expression> another : filtersAboveEachConsumer) {
                if (another.equals(someone)) {
                    matchCount++;
                    continue;
                }
                Set<Expression> matched = new HashSet<>();
                for (Expression e : another) {
                    Set<SlotReference> otherSlots = e.collect(ae -> ae instanceof SlotReference);
                    if (otherSlots.equals(slots)) {
                        matched.add(e);
                    }
                }
                if (!matched.isEmpty()) {
                    matchCount++;
                }
                mightBeJoined.addAll(matched);
            }
            if (matchCount >= filterSize) {
                mightBeJoined.add(f);
                conjuncts.add(ExpressionUtils.or(mightBeJoined));
            }
        }
        if (!conjuncts.isEmpty()) {
            // distinct conjuncts
            ImmutableSet.Builder<Expression> newConjuncts = ImmutableSet.builderWithExpectedSize(conjuncts.size());
            for (Expression conjunct : conjuncts) {
                newConjuncts.addAll(ExpressionUtils.extractConjunction(conjunct));
            }
            LogicalPlan filter = new LogicalFilter<>(newConjuncts.build(), child);
            return pushPlanUnderAnchor(filter);
        }
        return child;
    }
}
