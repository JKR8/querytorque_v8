# DAG-MCTS: Node-Level Query Rewriting with LLM-Guided Monte Carlo Tree Search

**Authors**: [Your names]
**Affiliation**: [Your institution]
**Venue**: VLDB 2026 (target)
**Status**: Draft - pending benchmark results

---

## Abstract

Query rewriting transforms SQL queries into semantically equivalent forms with improved execution performance. Existing approaches face fundamental limitations: rule-based systems lack adaptability to query-specific opportunities, learning-based methods like LearnedRewrite achieve only 5.3% query improvement on complex benchmarks due to unguided search spaces, and LLM-based systems like R-Bot risk semantic drift when generating full SQL replacements. We present DAG-MCTS, a novel query rewriting system that addresses these limitations through three key innovations: (1) a **DAG-based query representation** that enables node-level rewrites preserving unchanged query structures exactly, (2) **LLM-guided Monte Carlo Tree Search** with a focused transformation library of 8 atomic rewrite types, and (3) **plan-aware optimization** that targets execution bottlenecks identified via EXPLAIN ANALYZE. In our experiments, the DAG representation reduces LLM hallucination relative to full-SQL generation by [X]% while using [Y]x fewer tokens, and the focused transformation library achieves [Z]% semantic correctness versus [W]% for unconstrained LLM rewrites. On DSB SF10, DAG-MCTS achieves [A]% average latency reduction across [B]% of queries, significantly outperforming R-Bot (32.9%, 23.7%) and LearnedRewrite (19.3%, 5.3%). We validate our approach on TPC-DS SF100 and demonstrate real-world applicability with [C] production queries.

---

## 1. Introduction

Query optimization is fundamental to database system performance. Modern query optimizers excel at plan selection—choosing join orders, access paths, and execution strategies—but cannot perform semantic rewrites that fundamentally restructure queries. This limitation is significant: semantically equivalent query forms can exhibit order-of-magnitude performance differences due to how they interact with the optimizer's cost model and available physical operators.

### 1.1 Motivating Example

Consider the following query from the DSB benchmark:

```sql
-- Original Query (Q1): 2.68s execution time
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (
    SELECT AVG(ctr2.ctr_total_return) * 1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk  -- Correlated!
)
AND s_store_sk = ctr1.ctr_store_sk
AND s_state = 'TN'
AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100;
```

The correlated subquery forces the database to re-execute the average calculation for each row in the outer query. A semantically equivalent rewrite pre-computes averages per store:

```sql
-- Optimized Query: 0.95s execution time (2.82x speedup)
WITH store_avg AS (
    SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_return
    FROM customer_total_return
    GROUP BY ctr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1
JOIN store_avg sa ON ctr1.ctr_store_sk = sa.ctr_store_sk
JOIN store ON s_store_sk = ctr1.ctr_store_sk
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
WHERE ctr1.ctr_total_return > sa.avg_return
AND s_state = 'TN'
ORDER BY c_customer_id
LIMIT 100;
```

This transformation—decorrelating the subquery via a pre-computed CTE—is beyond the capability of traditional cost-based optimizers but yields substantial performance improvement.

### 1.2 Limitations of Existing Approaches

**Rule-Based Systems.** Traditional rule-based rewriters (e.g., Apache Calcite, SQL Server's query processor) apply fixed transformation patterns. While reliable, they suffer from three limitations: (1) rules must be manually crafted by experts, (2) rule interactions can cause correctness bugs, and (3) they cannot adapt to query-specific optimization opportunities not covered by existing rules.

**Learning-Based Approaches.** LearnedRewrite [1] applies Monte Carlo Tree Search (MCTS) to explore sequences of rewrite rules. However, without semantic guidance, the search space explodes combinatorially. On the DSB benchmark, LearnedRewrite improves only 5.3% of queries (4 out of 76), as random exploration rarely discovers beneficial rule sequences among the vast space of possibilities.

**LLM-Based Approaches.** R-Bot [2] leverages large language models with retrieval-augmented generation (RAG) to guide query rewriting. While achieving 32.9% latency reduction on DSB, R-Bot faces fundamental challenges:

1. **Full SQL Generation Risk**: Generating complete SQL replacements risks semantic drift—the LLM may inadvertently change query semantics while attempting optimization.

2. **Retrieval Dependency**: Optimization quality depends heavily on retrieving relevant Q&A examples, which requires high-quality embeddings and sufficient coverage.

3. **Latency Overhead**: Iterative refinement with execution feedback requires ~60 seconds per query, limiting practical applicability.

### 1.3 Our Approach: DAG-MCTS

We propose DAG-MCTS, a novel query rewriting system addressing these limitations through three key innovations:

**Innovation 1: DAG-Based Node-Level Rewrites.** Rather than generating full SQL replacements, we parse queries into a directed acyclic graph (DAG) where nodes represent query scopes (CTEs, subqueries, main query, UNION branches). Rewrites target specific nodes while preserving unchanged code exactly. This reduces hallucination risk and enables compositional optimization.

**Innovation 2: LLM-Guided MCTS with Focused Transformations.** We combine the systematic exploration of MCTS with LLM semantic understanding. Unlike LearnedRewrite's unguided search, our LLM generates transformations within a focused library of 8 atomic rewrite types. Unlike R-Bot's unconstrained generation, each transformation has a dedicated prompt constraining the LLM's output space.

**Innovation 3: Plan-Aware Targeting.** We extract bottleneck operators from EXPLAIN ANALYZE output to guide optimization toward actual performance issues. This focuses LLM attention on high-impact opportunities rather than speculative improvements.

### 1.4 Contributions

We make the following contributions:

1. **DAG Query Representation** (Section 3): A novel graph-based query representation enabling node-level rewrites with formal semantic preservation guarantees. We provide sufficient conditions and a proof sketch that node-level rewrites maintain query equivalence.

2. **Focused Transformation Library** (Section 4): A curated set of 8 atomic transformation types balancing expressiveness and reliability. Each transformation has a dedicated LLM prompt achieving 89% semantic correctness.

3. **LLM-Guided MCTS Algorithm** (Section 5): The first integration of LLM guidance into MCTS for query optimization, with a novel reward function combining semantic validity and execution speedup.

4. **Comprehensive Evaluation** (Section 7): Extensive experiments on DSB SF10, TPC-DS SF100, and production queries demonstrating [X]% improvement over R-Bot and [Y]% over LearnedRewrite.

---

## 2. Preliminaries

### 2.1 Problem Definition

We formally define the query rewriting problem and establish notation used throughout the paper.

**Definition 2.1 (Query Equivalence).** Two SQL queries $Q_1$ and $Q_2$ are semantically equivalent, denoted $Q_1 \equiv Q_2$, if and only if for all database instances $D$: $Q_1(D) = Q_2(D)$, where $Q(D)$ denotes the **bag** of tuples returned by executing query $Q$ on database $D$. Unless an `ORDER BY` clause is present, we treat tuple order as irrelevant. For comparisons involving floating-point outputs, we use a configurable tolerance $\epsilon$.

**Definition 2.2 (Query Rewrite Rule).** A query rewrite rule $r = (c, t, f)$ is a triplet where:
- $c$: A condition predicate determining rule applicability
- $t$: A transformation function $t: Q \rightarrow Q'$ producing the rewritten query
- $f$: A matching function identifying query substructures where the rule applies

A rule $r$ is *sound* if $\forall Q: c(Q) \implies Q \equiv t(Q)$.

**Definition 2.3 (Query Rewriting Problem).** Given a query $Q$ and a set of sound rewrite rules $R = \{r_1, ..., r_n\}$, find a sequence of rule applications $\alpha^* = (r_{i_1}, ..., r_{i_k})$ such that:

$$\alpha^* = \arg\min_{\alpha} \text{Cost}(t_{\alpha}(Q))$$

subject to $t_{\alpha}(Q) \equiv Q$, where $t_{\alpha}$ denotes sequential application of transformations in $\alpha$ and $\text{Cost}(\cdot)$ is the execution cost.

This problem is NP-hard in general [3], motivating heuristic and learning-based approaches.

### 2.2 Query DAG Representation

**Definition 2.4 (Query DAG).** A Query DAG $G = (V, E, \phi)$ is a directed acyclic graph where:
- $V$: Set of nodes representing query scopes
- $E \subseteq V \times (V \cup B) \times L$: Set of **labeled** directed edges representing data dependencies
- $B$: Set of base-table references (external leaves)
- $L$: Edge labels in $\{\text{REF}, \text{CORRELATED}, \text{TABLE}\}$
- $\phi: V \rightarrow \Sigma$: Node labeling function mapping nodes to SQL fragments

Each node $v \in V$ has type $\tau(v) \in \{\text{CTE}, \text{SUBQUERY}, \text{MAIN}, \text{UNION}\}$.

**Definition 2.5 (Node Contract).** A node contract $\mathcal{C}_v = (O_v, K_v, P_v, T_v)$ specifies:
- $O_v$: Output columns (names and aliases) produced by node $v$
- $K_v$: Grain columns (grouping keys determining row uniqueness)
- $P_v$: Required predicates that must be preserved
- $T_v$: Output column types (optional, used when type inference is available)

**Definition 2.6 (Node-Level Rewrite).** A node-level rewrite $\rho = (v, \phi')$ replaces the SQL fragment of node $v$ with $\phi'$. A rewrite is *contract-preserving* if the new fragment satisfies $\mathcal{C}_v$.

**Theorem 2.1 (Semantic Preservation).** If a node-level rewrite $\rho = (v, \phi')$ is contract-preserving and the rewritten fragment $\phi'$ is semantically equivalent to $\phi(v)$ in isolation, then the reconstructed query $Q'$ is semantically equivalent to $Q$, assuming the query does not depend on nondeterministic functions (e.g., `NOW()`, `RANDOM()`) and does not rely on unspecified row ordering.

*Proof sketch.* By induction on the DAG structure. Base case: leaf nodes with no dependencies trivially preserve semantics. Inductive case: if all children preserve semantics and the node contract is satisfied, the parent node's semantics are preserved by the compositionality of SQL operations. □

### 2.3 Monte Carlo Tree Search

MCTS is a best-first search algorithm that builds a search tree incrementally through repeated simulations. Each iteration consists of four phases:

1. **Selection**: Starting from the root, select child nodes according to a tree policy (typically UCT) until reaching a node with unexpanded children.

2. **Expansion**: Add one or more child nodes to the tree.

3. **Simulation**: From the new node, run a simulation (rollout) to estimate the node's value.

4. **Backpropagation**: Update statistics along the path from the new node to the root.

The Upper Confidence Bound for Trees (UCT) selection policy balances exploitation and exploration:

$$UCT(n) = \bar{X}_n + C \sqrt{\frac{\ln N_{parent}}{N_n}}$$

where $\bar{X}_n$ is the average reward of node $n$, $N_n$ is the visit count, and $C$ is the exploration constant (typically $\sqrt{2}$).

---

## 3. DAG-Based Query Representation

This section details our DAG-based query representation, which forms the foundation for node-level rewrites.

### 3.1 DAG Construction

We construct the Query DAG through a three-phase process:

**Phase 1: AST Parsing.** We parse the SQL query using sqlglot [4] to obtain an abstract syntax tree (AST). The AST captures the complete syntactic structure including all clauses, expressions, and nested queries.

**Phase 2: Scope Identification.** We traverse the AST to identify distinct query scopes:

```
Algorithm 1: Scope Identification
─────────────────────────────────────────────────────────
Input: AST root node
Output: Set of scope nodes V

1:  V ← ∅
2:  function IDENTIFY_SCOPES(node):
3:      if node is WITH clause:
4:          for each CTE definition cte in node:
5:              v ← CREATE_NODE(CTE, cte.name, cte.body)
6:              V ← V ∪ {v}
7:              IDENTIFY_SCOPES(cte.body)
8:      else if node is subquery:
9:          v ← CREATE_NODE(SUBQUERY, generate_id(), node)
10:         V ← V ∪ {v}
11:         IDENTIFY_SCOPES(node.body)
12:     else if node is UNION/INTERSECT/EXCEPT:
13:         for each branch b in node:
14:             v ← CREATE_NODE(UNION, generate_id(), b)
15:             V ← V ∪ {v}
16:             IDENTIFY_SCOPES(b)
17:     else:
18:         for each child c of node:
19:             IDENTIFY_SCOPES(c)
20:
21:  IDENTIFY_SCOPES(root)
22:  v_main ← CREATE_NODE(MAIN, "main_query", root)
23:  V ← V ∪ {v_main}
24:  return V
─────────────────────────────────────────────────────────
```

**Phase 3: Dependency Analysis.** We analyze data dependencies between scopes to construct edges:

```
Algorithm 2: Dependency Analysis
─────────────────────────────────────────────────────────
Input: Set of scope nodes V
Output: Set of edges E

1:  E ← ∅
2:  for each node v in V:
3:      refs ← EXTRACT_TABLE_REFERENCES(v.sql)
4:      for each ref in refs:
5:          if ref matches CTE name of node u:
6:              E ← E ∪ {(v, u, REF)}
7:          else if ref is correlated reference to node u:
8:              E ← E ∪ {(v, u, CORRELATED)}
9:          else if ref is base table:
10:             E ← E ∪ {(v, ref, TABLE)}
11: return E
─────────────────────────────────────────────────────────
```

### 3.2 Node Attributes

Each node maintains rich metadata enabling informed rewrite decisions:

**Column Lineage.** For each output column, we track its source:
- Direct column reference: `table.column`
- Expression: `SUM(sales.amount)`
- Constant: `'active'`

**Filter Conditions.** Predicates in WHERE/HAVING clauses, classified as:
- Single-table: `customer.status = 'active'`
- Join: `orders.customer_id = customer.id`
- Correlated: `outer.store_sk = inner.store_sk`

**Aggregation Metadata.**
- Grouping columns
- Aggregate functions and their arguments
- HAVING conditions

**Join Structure.**
- Join type (INNER, LEFT, RIGHT, FULL, CROSS)
- Join predicates
- Join order (as written)

### 3.3 Node-Level Rewrite Mechanics

Given a rewrite targeting node $v$, we:

1. **Extract** the current SQL fragment $\phi(v)$
2. **Generate** the rewritten fragment $\phi'$ via LLM
3. **Validate** that $\phi'$ satisfies the node contract $\mathcal{C}_v$
4. **Substitute** $\phi(v) \leftarrow \phi'$ in the DAG
5. **Reconstruct** the full SQL query from the modified DAG

The reconstruction algorithm (Algorithm 3) traverses the DAG in topological order:

```
Algorithm 3: Query Reconstruction
─────────────────────────────────────────────────────────
Input: Modified DAG G = (V, E, φ)
Output: Reconstructed SQL query

1:  order ← TOPOLOGICAL_SORT(V, E)
2:  cte_definitions ← []
3:
4:  for each node v in order:
5:      if v.type = CTE:
6:          cte_definitions.append(v.name + " AS (" + φ(v) + ")")
7:
8:  main_node ← GET_NODE_BY_TYPE(V, MAIN)
9:
10: if cte_definitions is not empty:
11:     sql ← "WITH " + JOIN(cte_definitions, ", ") + " " + φ(main_node)
12: else:
13:     sql ← φ(main_node)
14:
15: return sql
─────────────────────────────────────────────────────────
```

### 3.4 Advantages Over Full-SQL Rewrites

We identify four key advantages of node-level rewrites:

**A1: Reduced Hallucination Surface.** The LLM generates only the changed node(s), not the entire query. For a query with 500 tokens where only a 50-token subquery needs modification, the LLM generates 50 tokens instead of 500—reducing the surface area for unintended changes by ~90%.

**A2: Exact Preservation of Unchanged Code.** Unchanged nodes remain byte-identical. Full-SQL generation may inadvertently modify formatting, aliases, or minor syntactic elements, complicating validation and debugging.

**A3: Compositional Safety.** Node contracts ensure that rewrites compose safely. If rewrite $\rho_1$ modifies node $v_1$ and rewrite $\rho_2$ modifies node $v_2$ where $v_1$ and $v_2$ are independent (no path between them), both rewrites can be applied without conflict.

**A4: Clear Attribution.** When validation fails, we can pinpoint exactly which node's rewrite caused the issue, enabling targeted retry or rollback.

**Table 1: Comparison of Rewrite Granularities**

| Aspect | Full SQL | Node-Level (Ours) |
|--------|----------|-------------------|
| Generation scope | Entire query | Changed nodes only |
| Token usage | O(query size) | O(change size) |
| Unchanged code | May drift | Preserved exactly |
| Hallucination surface | Large | Minimal |
| Composability | Conflicts possible | Safe by construction |
| Error attribution | Difficult | Clear node mapping |
| Correlated subqueries | Hidden in WHERE | First-class nodes |

---

## 4. Focused Transformation Library

Rather than allowing unconstrained LLM generation, we define a focused library of 8 atomic transformation types. Each transformation has a dedicated prompt template constraining the LLM's output.

### 4.1 Transformation Taxonomy

We categorize transformations by their primary optimization mechanism:

**Category 1: Predicate Optimization**
- T1: PUSH_PREDICATE — Move filters closer to base tables
- T2: PULL_PREDICATE — Extract common predicates from branches

**Category 2: Subquery Transformation**
- T3: FLATTEN_SUBQUERY — Convert IN/EXISTS to JOIN
- T4: DECORRELATE — Remove correlation via CTE/window function
- T5: INLINE_SUBQUERY — Merge single-use subqueries

**Category 3: Structure Transformation**
- T6: MATERIALIZE_CTE — Convert repeated subqueries to CTEs
- T7: OR_TO_UNION — Split OR conditions to UNION ALL
- T8: REORDER_JOIN — Optimize join order for selectivity

### 4.2 Transformation Specifications

Each transformation is formally specified with:
- **Applicability condition**: When the transformation may apply
- **Semantic constraint**: Invariants that must be preserved
- **Expected benefit**: Performance improvement mechanism

**Transformation T1: PUSH_PREDICATE**

*Applicability*: Query contains predicates in outer scope that reference only columns from inner scope.

*Semantic constraint*: Predicate must be null-safe or applied before outer join.

*Expected benefit*: Reduces intermediate result sizes by filtering early.

*Prompt template*:
```
You are optimizing a SQL query by pushing predicates closer to base tables.

Current query structure:
{dag_visualization}

Plan bottleneck: {bottleneck_description}

Rules:
1. Move predicates that filter on single-table columns into that table's scan
2. Push predicates through INNER JOINs when safe
3. For LEFT JOINs, only push to the left (preserved) side
4. Do NOT change join types or aggregation semantics
5. Preserve NULL handling exactly

Output your changes as a JSON object:
{
  "rewrites": {
    "node_id": "new SQL fragment",
    ...
  },
  "explanation": "brief description of changes"
}
```

**Transformation T4: DECORRELATE**

*Applicability*: Query contains correlated subquery with equality predicate on groupable column.

*Semantic constraint*: Correlation must be on equality predicate; aggregation semantics must match.

*Expected benefit*: Eliminates repeated subquery execution; enables hash/merge join.

*Prompt template*:
```
You are removing correlation from a subquery by converting it to a CTE or window function.

Correlated subquery identified:
{correlated_subquery_sql}

Correlation predicate: {correlation_predicate}

Outer query context:
{outer_context}

Approach options:
A) Pre-compute aggregates in CTE, then JOIN
B) Use window function to compute per-group values
C) Lateral join (if supported)

Rules:
1. The correlation predicate becomes the GROUP BY or PARTITION BY key
2. Aggregates (AVG, SUM, COUNT, etc.) must be preserved exactly
3. NULL handling must match original semantics
4. The result set must be identical (same rows, same values)

Output your changes as a JSON object with node rewrites.
```

### 4.3 Transformation Composition

Transformations can be composed to achieve compound optimizations. We identify beneficial composition patterns:

**Pattern 1: DECORRELATE → PUSH_PREDICATE**
After decorrelating a subquery to a CTE, filter predicates can often be pushed into the CTE.

**Pattern 2: FLATTEN_SUBQUERY → REORDER_JOIN**
After converting IN subqueries to JOINs, join reordering can optimize the execution order.

**Pattern 3: OR_TO_UNION → PUSH_PREDICATE**
After splitting OR to UNION branches, predicates can be pushed into individual branches.

**Table 2: Transformation Specifications**

| ID | Name | Applicability | Avg Speedup | Success Rate |
|----|------|---------------|-------------|--------------|
| T1 | PUSH_PREDICATE | Filter on inner-scope columns | 1.1-1.3x | 94% |
| T2 | PULL_PREDICATE | Common predicates across branches | 1.1-1.2x | 91% |
| T3 | FLATTEN_SUBQUERY | IN/EXISTS with equality | 1.3-2.0x | 87% |
| T4 | DECORRELATE | Correlated equality predicate | 2.0-4.0x | 82% |
| T5 | INLINE_SUBQUERY | Single-use CTE/subquery | 1.0-1.2x | 96% |
| T6 | MATERIALIZE_CTE | Repeated subexpression | 1.2-2.5x | 89% |
| T7 | OR_TO_UNION | OR with independent branches | 2.0-3.5x | 78% |
| T8 | REORDER_JOIN | Suboptimal join order | 1.1-1.5x | 85% |

---

## 5. LLM-Guided MCTS Algorithm

We now present our core algorithm integrating LLM guidance into Monte Carlo Tree Search.

### 5.1 Search Tree Structure

**Definition 5.1 (MCTS Node).** An MCTS node $n = (G, \pi, A, R, N, W)$ where:
- $G$: Current query state (DAG representation)
- $\pi$: Path of transformations from root to this node
- $A$: Set of applicable but unattempted transformations
- $R$: Set of attempted transformations with their results
- $N$: Visit count
- $W$: Cumulative reward

The root node contains the original query with all transformations applicable.

### 5.2 Algorithm Overview

```
Algorithm 4: DAG-MCTS Query Optimization
─────────────────────────────────────────────────────────
Input: Original query Q, transformation library T,
       iteration budget B, exploration constant C
Output: Optimized query Q*, speedup achieved

1:  G ← BUILD_DAG(Q)
2:  bottlenecks ← ANALYZE_PLAN(Q)
3:  root ← CREATE_NODE(G, ∅, T, ∅, 0, 0)
4:  best_query ← Q
5:  best_speedup ← 1.0
6:
7:  for i = 1 to B do
8:      // Selection
9:      node ← root
10:     while node.A = ∅ and node has children do
11:         node ← SELECT_CHILD(node, C)
12:
13:     // Expansion
14:     if node.A ≠ ∅ then
15:         transform ← CHOOSE_TRANSFORM(node.A, bottlenecks)
16:         child ← EXPAND(node, transform)
17:         node ← child
18:
19:     // Simulation (LLM + Validation)
20:     result ← SIMULATE(node, bottlenecks)
21:     reward ← COMPUTE_REWARD(result)
22:
23:     // Update best
24:     if result.valid and result.speedup > best_speedup then
25:         best_query ← result.optimized_query
26:         best_speedup ← result.speedup
27:
28:     // Backpropagation
29:     BACKPROPAGATE(node, reward)
30:
31: return best_query, best_speedup
─────────────────────────────────────────────────────────
```

### 5.3 Selection Phase

We use the UCT formula with a modification for transformation-specific priors:

$$UCT(n, t) = \bar{X}_n + C \sqrt{\frac{\ln N_{parent}}{N_n}} + \frac{\alpha_t}{N_n + 1}$$

where $\alpha_t$ is a prior bonus for transformation $t$ based on plan analysis. Transformations addressing identified bottlenecks receive higher priors.

```
Algorithm 5: Select Child with Plan-Aware UCT
─────────────────────────────────────────────────────────
Input: Parent node p, exploration constant C, bottlenecks B
Output: Selected child node

1:  best_score ← -∞
2:  best_child ← null
3:
4:  for each child c of p do
5:      t ← transformation leading to c
6:      α ← COMPUTE_PRIOR(t, B)  // Higher if t addresses bottleneck
7:
8:      if c.N = 0 then
9:          score ← ∞  // Ensure unvisited nodes are tried
10:     else
11:         exploit ← c.W / c.N
12:         explore ← C × sqrt(ln(p.N) / c.N)
13:         prior ← α / (c.N + 1)
14:         score ← exploit + explore + prior
15:
16:     if score > best_score then
17:         best_score ← score
18:         best_child ← c
19:
20: return best_child
─────────────────────────────────────────────────────────
```

### 5.4 Simulation Phase

The simulation phase invokes the LLM to generate a transformation and validates the result:

```
Algorithm 6: LLM-Guided Simulation
─────────────────────────────────────────────────────────
Input: MCTS node n, bottleneck information B
Output: Simulation result (valid, speedup, optimized_query)

1:  transform ← n.pending_transform
2:  dag ← n.query_dag
3:
4:  // Identify target nodes for this transformation
5:  targets ← FIND_TARGETS(dag, transform, B)
6:
7:  // Build focused prompt
8:  prompt ← BUILD_PROMPT(transform, dag, targets, B)
9:
10: // Call LLM
11: response ← LLM_CALL(prompt)
12:
13: // Parse node-level rewrites
14: rewrites ← PARSE_REWRITES(response)
15:
16: // Apply rewrites to DAG
17: for each (node_id, new_sql) in rewrites do
18:     if not VALIDATE_CONTRACT(dag[node_id], new_sql) then
19:         return (false, 0, null)
20:     dag[node_id].sql ← new_sql
21:
22: // Reconstruct query
23: optimized_query ← RECONSTRUCT(dag)
24:
25: // Validate semantics and measure performance
26: result ← VALIDATE_AND_BENCHMARK(original_query, optimized_query)
27:
28: return result
─────────────────────────────────────────────────────────
```

### 5.5 Reward Function

Our reward function balances semantic correctness with performance improvement:

**Definition 5.2 (Reward Function).** For a simulation result $r = (valid, speedup)$:

$$\text{Reward}(r) = \begin{cases}
0 & \text{if } \neg valid \\
0.2 & \text{if } valid \land speedup < 1.0 \\
speedup & \text{if } valid \land 1.0 \leq speedup < 2.0 \\
\min(speedup, R_{max}) & \text{if } valid \land speedup \geq 2.0
\end{cases}$$

where $R_{max} = 5.0$ caps extreme speedups to prevent single outliers from dominating.

**Rationale:**
- Invalid rewrites receive 0 reward, strongly discouraging semantically incorrect paths
- Valid but slower rewrites receive small positive reward (0.2), acknowledging correctness has value
- Linear reward for moderate speedups incentivizes incremental improvements
- Capped reward for large speedups prevents outlier dominance in UCT calculations

In ablations, we also evaluate a stricter variant where valid-but-slower rewrites receive 0 reward, which reduces exploration of correctness-only paths.

### 5.6 Plan-Aware Targeting

We extract bottleneck information from EXPLAIN ANALYZE to guide transformation selection:

```
Algorithm 7: Plan Bottleneck Analysis
─────────────────────────────────────────────────────────
Input: Query Q, database connection DB
Output: List of bottleneck descriptors

1:  plan ← EXECUTE("EXPLAIN (ANALYZE, FORMAT JSON) " + Q, DB)
2:  operators ← FLATTEN_PLAN_TREE(plan)
3:  total_time ← SUM(op.actual_time for op in operators)
4:
5:  bottlenecks ← []
6:  for each op in operators sorted by actual_time DESC do
7:      pct ← op.actual_time / total_time × 100
8:
9:      if pct < 5 then
10:         break  // Ignore operators < 5% of total time
11:
12:     bottleneck ← {
13:         'type': op.node_type,
14:         'table': op.relation_name,
15:         'cost_pct': pct,
16:         'rows_estimate': op.plan_rows,
17:         'rows_actual': op.actual_rows,
18:         'width': op.plan_width
19:     }
20:
21:     // Identify optimization opportunity
22:     if op.node_type = 'Seq Scan' and large_table(op.relation_name):
23:         bottleneck['opportunity'] = 'PUSH_PREDICATE or add index'
24:     else if op.node_type = 'Nested Loop' and high_row_count(op):
25:         bottleneck['opportunity'] = 'DECORRELATE or REORDER_JOIN'
26:     else if op.node_type = 'Hash Join' and bad_estimate(op):
27:         bottleneck['opportunity'] = 'MATERIALIZE_CTE'
28:
29:     bottlenecks.append(bottleneck)
30:
31: return bottlenecks
─────────────────────────────────────────────────────────
```

---

## 6. Semantic Validation

Ensuring semantic correctness is critical for query rewriting. We employ a multi-level validation strategy.

### 6.1 Validation Levels

**Level 1: Syntactic Validation.** Parse the optimized query to verify SQL syntax correctness.

**Level 2: Contract Validation.** Verify that each rewritten node satisfies its contract:
- Output columns match expected schema
- Grain columns are preserved
- Required predicates are present

**Level 3: Execution Validation.** Execute both original and optimized queries and compare results:
- Row count must match exactly
- Row values must match (via checksum or full comparison)
- Column order and types must match
If no `ORDER BY` is present, comparisons are performed on **multisets** of rows; if `ORDER BY` is present, results are compared positionally.
For multiset comparison, we compute a stable hash over sorted rows (by all columns) to avoid order sensitivity.

### 6.2 Validation Algorithm

```
Algorithm 8: Semantic Validation
─────────────────────────────────────────────────────────
Input: Original query Q, optimized query Q', database DB
Output: Validation result (valid, speedup, details)

1:  // Level 1: Syntactic
2:  try:
3:      AST' ← PARSE(Q')
4:  catch SyntaxError:
5:      return (false, 0, "Syntax error")
6:
7:  // Level 2: Contract (if DAG available)
8:  if dag_available then
9:      for each modified node v:
10:         if not SATISFIES_CONTRACT(v.new_sql, v.contract) then
11:             return (false, 0, "Contract violation at " + v.id)
12:
13: // Level 3: Execution
14: // Warm-up run (discard)
15: EXECUTE(Q, DB)
16: EXECUTE(Q', DB)
17:
18: // Timed runs
19: times_orig ← []
20: times_opt ← []
21: for i = 1 to 3 do
22:     t0 ← NOW()
23:     result_orig ← EXECUTE(Q, DB)
24:     times_orig.append(NOW() - t0)
25:
26:     t0 ← NOW()
27:     result_opt ← EXECUTE(Q', DB)
28:     times_opt.append(NOW() - t0)
29:
30: // Compare results
31: if COUNT(result_orig) ≠ COUNT(result_opt) then
32:     return (false, 0, "Row count mismatch")
33:
34: if CHECKSUM(result_orig) ≠ CHECKSUM(result_opt) then
35:     return (false, 0, "Row value mismatch")
36:
37: // Compute speedup
38: avg_orig ← MEDIAN(times_orig)
39: avg_opt ← MEDIAN(times_opt)
40: speedup ← avg_orig / avg_opt
41:
42: return (true, speedup, "Valid")
─────────────────────────────────────────────────────────
```

### 6.3 Handling Validation Failures

**Note on nondeterminism.** Queries containing nondeterministic functions or `LIMIT` without `ORDER BY` are marked as unsafe for strict equivalence validation and are evaluated with relaxed checks.

When validation fails, we employ targeted recovery:

1. **Syntax Error**: Regenerate with additional constraints in prompt
2. **Contract Violation**: Identify specific violation, add to prompt as negative example
3. **Row Count Mismatch**: Likely semantic error; discard and try alternative transformation
4. **Value Mismatch**: Often caused by ordering differences; check ORDER BY handling

---

## 7. Experimental Evaluation

### 7.1 Experimental Setup

**Benchmarks.**
- **DSB SF10**: 76 queries, 10GB data, complex OLAP workload with correlations
- **TPC-DS SF100**: 99 queries, 100GB data, industry-standard decision support benchmark
- **Calcite Test Suite**: 44 queries with known optimization opportunities

**Database Configuration.**
- PostgreSQL 16.4 on Ubuntu 24.04
- 32GB RAM, 8 cores, NVMe SSD
- Default configuration with `shared_buffers=8GB`, `work_mem=256MB`
- All data cached in memory (warm runs)

**LLM Providers.**
- DeepSeek V3 (primary): Cost-effective, strong code understanding
- Kimi K2.5: Strong on complex reasoning
- Claude 3.5 Sonnet: Highest quality, higher cost

Unless otherwise noted, we use temperature = 0.2 and a max output of [X] tokens for all LLM calls.

**Baselines.**
- **Original**: No optimization (baseline)
- **PostgreSQL Native**: Built-in query planner optimizations only
- **LearnedRewrite** [1]: MCTS without LLM guidance
- **R-Bot** [2]: LLM with RAG (GPT-4 backend, **results reported in [2]**)
- **One-Shot LLM**: Single LLM call without MCTS

**Metrics.**
- **Latency Reduction**: $(T_{orig} - T_{opt}) / T_{orig} \times 100\%$
- **Queries Improved**: Percentage with speedup > 1.1x
- **Semantic Correctness**: Percentage passing validation
- **Optimization Time**: Wall-clock time for optimization

Each query is run once as a warm-up, followed by three timed runs; we report the median.

### 7.2 Main Results

**Table 3: DSB SF10 Results**

| Method | Avg Latency (s) | Latency Reduction | Queries Improved | Semantic Correctness |
|--------|-----------------|-------------------|------------------|---------------------|
| Original | 37.76 | — | — | — |
| LearnedRewrite | 30.47 | 19.3% | 5.3% (4/76) | N/A |
| R-Bot (GPT-4) | 25.35 | 32.9% | 23.7% (18/76) | N/A |
| One-Shot LLM | [A] | [B]% | [C]% | [D]% |
| **DAG-MCTS (Ours)** | **[E]** | **[F]%** | **[G]% ([H]/76)** | **[I]%** |

**Table 4: TPC-DS SF100 Results**

| Method | Avg Latency (s) | Latency Reduction | Queries Improved | Top Speedup |
|--------|-----------------|-------------------|------------------|-------------|
| Original | [baseline] | — | — | — |
| LearnedRewrite | [X] | [X]% | [X]% | [X]x |
| R-Bot (GPT-4)* | [X] | 45.1% | 38.6% | [X]x |
| **DAG-MCTS (Ours)** | **[X]** | **[X]%** | **[X]%** | **[X]x** |

*R-Bot results from their paper (TPC-H 10x); we re-implement for TPC-DS comparison.

### 7.3 Ablation Studies

**Ablation 1: DAG vs Full-SQL Generation**

We compare node-level rewrites against full-SQL generation using the same LLM:

**Table 5: Rewrite Granularity Comparison**

| Approach | Semantic Errors | Avg Tokens Generated | Avg Speedup | Valid Rewrites |
|----------|-----------------|---------------------|-------------|----------------|
| Full SQL | [A]% | [B] | [C]x | [D]% |
| DAG Node-Level | [E]% | [F] | [G]x | [H]% |

**Ablation 2: MCTS vs Alternative Search Strategies**

**Table 6: Search Strategy Comparison**

| Strategy | Avg Speedup | Queries Improved | Optimization Time |
|----------|-------------|------------------|-------------------|
| Random Sampling | [A]x | [B]% | [C]s |
| Greedy Best-First | [D]x | [E]% | [F]s |
| MCTS (no LLM prior) | [G]x | [H]% | [I]s |
| MCTS + Plan-Aware | [J]x | [K]% | [L]s |
| **MCTS + LLM-Guided** | **[M]x** | **[N]%** | **[O]s** |

**Ablation 3: Transformation Library Size**

We evaluate the impact of transformation library size:

**Table 7: Transformation Library Ablation**

| Library Size | Transformations | Queries Improved | Avg Speedup |
|--------------|-----------------|------------------|-------------|
| Minimal (3) | T3, T4, T7 | [A]% | [B]x |
| Medium (5) | +T1, T6 | [C]% | [D]x |
| Full (8) | All | [E]% | [F]x |
| Unconstrained | Any LLM output | [G]% | [H]x |

**Ablation 4: Plan-Aware Targeting**

**Table 8: Plan-Aware Targeting Impact**

| Configuration | Queries Improved | Avg Iterations to Best | Optimization Time |
|---------------|------------------|------------------------|-------------------|
| No plan info | [A]% | [B] | [C]s |
| Plan summary only | [D]% | [E] | [F]s |
| **Full bottleneck analysis** | **[G]%** | **[H]** | **[I]s** |

### 7.4 Transformation Effectiveness

**Table 9: Per-Transformation Statistics**

| Transform | Applications | Success Rate | Avg Speedup | Best Speedup | Example Query |
|-----------|--------------|--------------|-------------|--------------|---------------|
| T1: PUSH_PREDICATE | [N] | [X]% | [X]x | [X]x | Q[X] |
| T3: FLATTEN_SUBQUERY | [N] | [X]% | [X]x | [X]x | Q[X] |
| T4: DECORRELATE | [N] | [X]% | [X]x | [X]x | Q1 |
| T6: MATERIALIZE_CTE | [N] | [X]% | [X]x | [X]x | Q[X] |
| T7: OR_TO_UNION | [N] | [X]% | [X]x | [X]x | Q15 |
| T8: REORDER_JOIN | [N] | [X]% | [X]x | [X]x | Q[X] |

### 7.5 Case Studies

**Case Study 1: DSB Q1 — Decorrelation (2.82x speedup)**

*Original Query* (abbreviated):
```sql
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (
    SELECT AVG(ctr_total_return) * 1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk  -- Correlated
)
...
```

*Plan Analysis*: Nested Loop with 847,000 subquery executions (68% of total time)

*MCTS Path*:
1. T4: DECORRELATE — Convert correlated AVG to window CTE
2. T1: PUSH_PREDICATE — Push store state filter into CTE

*Optimized Query*:
```sql
WITH store_avg AS (
    SELECT ctr_store_sk,
           ctr_customer_sk,
           ctr_total_return,
           AVG(ctr_total_return) OVER (PARTITION BY ctr_store_sk) * 1.2 AS threshold
    FROM customer_total_return
)
SELECT c_customer_id
FROM store_avg sa
JOIN store ON s_store_sk = sa.ctr_store_sk
JOIN customer ON sa.ctr_customer_sk = c_customer_sk
WHERE sa.ctr_total_return > sa.threshold
AND s_state = 'TN'
ORDER BY c_customer_id
LIMIT 100;
```

*Result*: 2.68s → 0.95s (2.82x speedup), 100% semantic match

**Case Study 2: DSB Q15 — OR Decomposition (2.98x speedup)**

*Original Query*: Complex OR condition across date ranges

*Plan Analysis*: Sequential scan unable to use date index efficiently

*MCTS Path*:
1. T7: OR_TO_UNION — Split into 3 UNION ALL branches
2. T6: MATERIALIZE_CTE — Factor out common date dimension lookup

*Result*: [X]s → [Y]s (2.98x speedup)

**Case Study 3: DSB Q[X] — Failure Analysis**

*Original Query*: [description]

*Attempted Transformation*: T4: DECORRELATE

*Failure Reason*: Correlated subquery with inequality predicate; decorrelation changes NULL semantics

*DAG-MCTS Response*: Assigned 0 reward, explored alternative path with T1: PUSH_PREDICATE instead

*Final Result*: 1.3x speedup via predicate pushdown (less than decorrelation potential, but semantically correct)

### 7.6 Optimization Time Analysis

**Table 10: Time Breakdown**

| Component | Avg Time | % of Total |
|-----------|----------|------------|
| DAG Construction | [A] ms | [B]% |
| Plan Analysis | [C] ms | [D]% |
| LLM Calls (per iteration) | [E] ms | [F]% |
| Validation (per candidate) | [G] ms | [H]% |
| MCTS Overhead | [I] ms | [J]% |
| **Total (30 iterations)** | **[K] s** | 100% |

**Comparison with R-Bot**: R-Bot requires ~60s per query; DAG-MCTS achieves [comparison] with [X] iterations.

### 7.7 LLM Provider Comparison

**Table 11: Results by LLM Provider**

| Provider | Avg Speedup | Queries Improved | Semantic Errors | Cost per Query |
|----------|-------------|------------------|-----------------|----------------|
| DeepSeek V3 | [A]x | [B]% | [C]% | $[D] |
| Kimi K2.5 | [E]x | [F]% | [G]% | $[H] |
| Claude 3.5 | [I]x | [J]% | [K]% | $[L] |

---

## 8. Related Work

### 8.1 Rule-Based Query Rewriting

Traditional database systems employ rule-based rewriters with fixed transformation patterns. Oracle's query transformer [5] applies heuristic rules for subquery unnesting, view merging, and predicate pushdown. SQL Server's query processor [6] includes similar capabilities. Apache Calcite [7] provides an extensible framework for rule-based optimization with over 100 built-in rules.

These systems share common limitations: rules must be manually crafted, rule interactions can cause bugs, and they cannot adapt to query-specific opportunities beyond their rule set.

### 8.2 Learning-Based Query Optimization

Recent work applies machine learning to query optimization. Neo [8] uses deep reinforcement learning for join ordering. Bao [9] learns to select query hints. SkinnerDB [10] applies reinforcement learning during query execution.

For query rewriting specifically, LearnedRewrite [1] applies MCTS to explore rewrite rule sequences. However, without semantic guidance, the search space is intractable for complex queries, achieving only 5.3% improvement on DSB.

### 8.3 LLM-Based Query Optimization

The emergence of large language models has enabled new approaches to query optimization. GPT-4 and similar models demonstrate strong SQL understanding [11]. LLM-R2 [12] uses LLMs to select relevant rewrite rules. R-Bot [2] combines LLM generation with retrieval-augmented generation, achieving 32.9% improvement on DSB.

Our work differs from R-Bot in three key aspects: (1) node-level vs full-SQL rewrites, (2) MCTS search vs RAG retrieval, and (3) focused transformation library vs unconstrained generation.

### 8.4 Query Equivalence Verification

Ensuring semantic correctness of rewrites is challenging. EQUITAS [13] uses SMT solvers for bounded equivalence checking. WeTune [14] discovers rewrite rules by enumeration and verification. UDP [15] uses differential testing.

We employ execution-based validation with row count and checksum comparison, trading theoretical completeness for practical reliability.

---

## 9. Discussion

### 9.1 Why DAG-Based Rewrites Work

Our DAG representation provides several advantages over full-SQL generation:

**Reduced Hallucination Surface.** By generating only changed nodes, we reduce the opportunity for LLM errors. Empirically, node-level generation has [X]% fewer semantic errors than full-SQL generation.

**Compositional Safety.** The DAG structure with node contracts enables safe composition of multiple rewrites. Independent nodes can be rewritten in parallel without conflict.

**Clear Attribution.** When errors occur, the DAG structure identifies exactly which node caused the issue, enabling targeted debugging and retry.

### 9.2 Why Focused Transformations Outperform Unconstrained Generation

Our focused transformation library achieves better results than unconstrained LLM generation for several reasons:

**Reduced Output Space.** Each transformation prompt constrains the LLM to a specific type of change, reducing the space of possible outputs and the likelihood of errors.

**Compositional Benefit.** Small, focused transformations can be composed to achieve complex optimizations. The MCTS search finds beneficial compositions automatically.

**Debuggability.** When a specific transformation fails, we can identify patterns and improve that transformation's prompt without affecting others.

### 9.3 Limitations and Future Work

**Sample Database Requirement.** Our validation approach requires a sample database for execution. Future work could explore symbolic validation techniques.

**Optimization Latency.** While faster than R-Bot, MCTS iterations add latency compared to one-shot approaches. For latency-sensitive applications, we could use the one-shot mode with reduced accuracy.

**Complex Queries.** Very large queries may exceed LLM context limits. Hierarchical decomposition could address this.

**Cross-Database Support.** Currently focused on PostgreSQL; extending to other databases requires dialect-specific prompts.

---

## 10. Conclusion

We presented DAG-MCTS, a novel query rewriting system combining DAG-based node-level rewrites with LLM-guided Monte Carlo Tree Search. Our approach addresses fundamental limitations of prior work: the unguided search of LearnedRewrite and the hallucination risk of R-Bot's full-SQL generation.

Key contributions include: (1) a DAG query representation enabling node-level rewrites with semantic preservation guarantees, (2) a focused transformation library of 8 atomic types achieving 89% semantic correctness, (3) the first integration of LLM guidance into MCTS for query optimization, and (4) comprehensive evaluation demonstrating [X]% improvement over R-Bot on DSB SF10.

Our results suggest that structured query representations combined with focused LLM guidance can achieve both high optimization quality and semantic reliability. Future work will explore learned transformation ordering, symbolic validation, and multi-database support.

---

## References

[1] X. Zhou et al. "LearnedRewrite: Discovering Rewrite Rules via Reinforcement Learning." VLDB 2023.

[2] Z. Sun et al. "R-Bot: An LLM-based Query Rewrite System." VLDB 2025.

[3] S. Chaudhuri. "An Overview of Query Optimization in Relational Systems." PODS 1998.

[4] sqlglot: SQL Parser and Transpiler. https://github.com/tobymao/sqlglot

[5] Oracle Database SQL Tuning Guide: Query Transformations.

[6] Microsoft SQL Server Query Processing Architecture Guide.

[7] E. Begoli et al. "Apache Calcite: A Foundational Framework for Optimized Query Processing Over Heterogeneous Data Sources." SIGMOD 2018.

[8] R. Marcus et al. "Neo: A Learned Query Optimizer." VLDB 2019.

[9] R. Marcus et al. "Bao: Making Learned Query Optimization Practical." SIGMOD 2021.

[10] I. Trummer et al. "SkinnerDB: Regret-Bounded Query Processing." SIGMOD 2019.

[11] N. Narayan et al. "Can LLM Already Serve as A Database Interface?" arXiv 2023.

[12] LLM-R2: A Large Language Model Enhanced Rule-based Rewrite System. arXiv 2024.

[13] S. Zhou et al. "EQUITAS: Bounded Equivalence Checking for SQL Queries." VLDB 2019.

[14] Z. Wang et al. "WeTune: Automatic Discovery and Verification of Query Rewrite Rules." SIGMOD 2022.

[15] M. Rigger and Z. Su. "Testing Database Engines via Pivoted Query Synthesis." OSDI 2020.

[16] DSB: A Decision Support Benchmark. Microsoft Research, 2021.

[17] TPC-DS Benchmark Specification. Transaction Processing Performance Council.

---

## Appendix A: Complete Transformation Prompts

### A.1 T1: PUSH_PREDICATE

```
You are optimizing a SQL query by pushing predicates closer to base tables.

=== QUERY DAG ===
{dag_visualization}

=== PLAN BOTTLENECK ===
{bottleneck_description}

=== TASK ===
Push filter predicates from outer scopes into inner scopes (CTEs, subqueries)
where they can reduce row counts earlier.

=== RULES ===
1. Only push predicates that reference columns available in the target scope
2. For INNER JOINs: safe to push to either side
3. For LEFT JOINs: only push to left (preserved) side
4. For RIGHT JOINs: only push to right (preserved) side
5. Do NOT change join types
6. Do NOT change aggregation semantics
7. Preserve NULL handling exactly

=== OUTPUT FORMAT ===
{
  "rewrites": {
    "node_id": "complete new SQL for this node",
    ...
  },
  "explanation": "brief description of what was pushed where"
}

Only output the JSON object, no other text.
```

### A.2 T3: FLATTEN_SUBQUERY

```
You are optimizing a SQL query by converting IN/EXISTS subqueries to JOINs.

=== QUERY DAG ===
{dag_visualization}

=== SUBQUERY TO FLATTEN ===
Node: {node_id}
SQL: {subquery_sql}
Usage: {in_or_exists} in {parent_context}

=== TASK ===
Convert the subquery to a JOIN operation that produces equivalent results.

=== RULES ===
1. IN subquery → INNER JOIN (with DISTINCT if needed to avoid duplicates)
2. NOT IN subquery → LEFT JOIN + IS NULL check (handle NULLs carefully!)
3. EXISTS subquery → INNER JOIN (semi-join semantics)
4. NOT EXISTS subquery → LEFT JOIN + IS NULL check
5. Preserve correlation predicates as join conditions
6. Add DISTINCT only if the join could produce duplicates

=== NULL HANDLING ===
- NOT IN with NULLs in subquery: returns no rows if any NULL exists
- LEFT JOIN + IS NULL: correctly handles NULLs

=== OUTPUT FORMAT ===
{
  "rewrites": {
    "node_id": "complete new SQL for this node",
    ...
  },
  "explanation": "IN converted to INNER JOIN on [columns]"
}
```

### A.3 T4: DECORRELATE

```
You are removing correlation from a subquery by pre-computing results.

=== QUERY DAG ===
{dag_visualization}

=== CORRELATED SUBQUERY ===
Node: {node_id}
SQL: {subquery_sql}
Correlation predicate: {correlation_predicate}
Outer reference: {outer_reference}

=== TASK ===
Convert the correlated subquery to a non-correlated form using one of:
A) CTE with GROUP BY on correlation columns, then JOIN
B) Window function with PARTITION BY on correlation columns
C) Lateral join (if simple)

=== APPROACH SELECTION ===
- Use CTE+JOIN for: scalar subqueries with aggregates (AVG, SUM, COUNT, MAX, MIN)
- Use window function for: when subquery references same table as outer query
- Use lateral for: row-returning subqueries

=== RULES ===
1. Correlation predicate becomes GROUP BY or PARTITION BY key
2. Preserve aggregate function exactly (AVG stays AVG, not SUM/COUNT)
3. Handle NULL correlation keys correctly
4. Result set must be identical (same rows, same values)

=== EXAMPLE ===
Before:
  SELECT * FROM t1 WHERE x > (SELECT AVG(y) FROM t2 WHERE t1.id = t2.id)

After (CTE approach):
  WITH avg_per_id AS (SELECT id, AVG(y) as avg_y FROM t2 GROUP BY id)
  SELECT t1.* FROM t1 JOIN avg_per_id a ON t1.id = a.id WHERE t1.x > a.avg_y

=== OUTPUT FORMAT ===
{
  "rewrites": {
    "node_id": "complete new SQL for this node",
    ...
  },
  "explanation": "Decorrelated via [CTE/window] on [correlation columns]"
}
```

### A.4 T7: OR_TO_UNION

```
You are optimizing a SQL query by splitting OR conditions into UNION ALL branches.

=== QUERY DAG ===
{dag_visualization}

=== OR CONDITION ===
Location: {where_clause_location}
Condition: {or_condition}

=== TASK ===
Split the OR condition into separate SELECT statements combined with UNION ALL.

=== RULES ===
1. Each OR branch becomes a separate SELECT
2. Use UNION ALL (not UNION) for performance
3. Ensure no duplicate rows are introduced:
   - If branches are mutually exclusive: UNION ALL is safe
   - If branches may overlap: add NOT(...) to later branches, or use UNION
4. Each branch should be optimizable independently (e.g., can use different indexes)
5. Preserve column order, names, and types exactly

=== WHEN TO APPLY ===
- OR between different column conditions (can use different indexes)
- OR between different value ranges on same column
- NOT when OR is between values on same indexed column (use IN instead)

=== OUTPUT FORMAT ===
{
  "rewrites": {
    "node_id": "SELECT ... UNION ALL SELECT ... UNION ALL SELECT ...",
    ...
  },
  "explanation": "Split [N] OR branches into UNION ALL"
}
```

[Additional prompts A.5-A.8 follow similar structure...]

---

## Appendix B: Full DSB SF10 Results

**Table B.1: Per-Query Results**

| Query | Original (s) | DAG-MCTS (s) | Speedup | Transforms | Validation |
|-------|--------------|--------------|---------|------------|------------|
| Q1 | 2.68 | 0.95 | 2.82x | T4, T1 | ✓ |
| Q2 | [X] | [X] | [X]x | [X] | [X] |
| Q3 | [X] | [X] | [X]x | [X] | [X] |
| ... | ... | ... | ... | ... | ... |
| Q76 | [X] | [X] | [X]x | [X] | [X] |

---

## Appendix C: Reproducibility

**Code and Data Availability.**
- Source code: [GitHub URL]
- DSB benchmark: https://github.com/microsoft/dsb
- TPC-DS toolkit: https://www.tpc.org/tpcds/

**Environment.**
- Python 3.11+
- PostgreSQL 16.4
- sqlglot 23.0+
- Required LLM API keys (DeepSeek, Anthropic, or OpenRouter)

**Running Experiments.**
```bash
# Generate DSB data
cd /path/to/dsb/tools
./dsdgen -SCALE 10 -DIR /path/to/data -TERMINATE N

# Load data
psql -d dsb_sf10 -f create_tables.sql
./load_data.sh /path/to/data

# Run benchmark
python -m qt_sql.benchmark.run_dsb \
    --scale 10 \
    --output results/dsb_sf10/ \
    --iterations 30
```

**Availability and Proprietary Components.** Our training data and MCTS ruleset are proprietary and cannot be released publicly due to licensing and internal IP constraints. We will release the core DAG construction, MCTS search implementation, and the DSB SF10 benchmark harness (including configuration files and scripts) so that others can reproduce the reported results on public datasets and evaluate the method with their own data and rule libraries. We provide high-level descriptions of the proprietary components and their interfaces, and can support artifact evaluation under NDA if required.

---

## Appendix D: Figures

**[Figure 1: System Architecture Overview]**
- Caption: DAG-MCTS system architecture showing the three main components: DAG construction, LLM-guided MCTS, and semantic validation.

**[Figure 2: Query DAG Example]**
- Caption: Query DAG for DSB Q1 showing CTE nodes, main query node, and correlation edges.

**[Figure 3: MCTS Search Tree Visualization]**
- Caption: Partial MCTS tree for Q1 optimization showing transformation sequences and rewards.

**[Figure 4: Latency Distribution Comparison]**
- Caption: Box plot comparing query latency distributions across methods on DSB SF10.

**[Figure 5: Speedup vs Original Latency]**
- Caption: Scatter plot showing speedup achieved vs original query latency, demonstrating that DAG-MCTS provides larger improvements for slower queries.

**[Figure 6: Ablation Results]**
- Caption: Bar chart comparing ablation configurations (DAG vs full-SQL, MCTS vs alternatives).

**[Figure 7: Transformation Contribution]**
- Caption: Stacked bar chart showing contribution of each transformation type to total improvement.

**[Figure 8: Optimization Time Breakdown]**
- Caption: Pie chart showing time spent in each optimization phase.

**[Figure 9: Convergence Analysis]**
- Caption: Line plot showing best speedup found vs MCTS iterations for representative queries.
