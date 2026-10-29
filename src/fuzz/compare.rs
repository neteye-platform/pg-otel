//! Compares the plan tree of `EXPLAIN (ANALYZE, FORMAT JSON)` with the spans
//! pg_otel produced for the same execution.
//!
//! Both sides read the same executor instrumentation, so the numbers must
//! agree up to the rounding `EXPLAIN` applies when it prints them.

use std::collections::HashMap;

use opentelemetry::{SpanId, Value};
use opentelemetry_sdk::trace::SpanData;
use serde_json::Value as Json;

/// One disagreement between `EXPLAIN` and the spans.
#[derive(Debug)]
pub struct Mismatch {
    /// Which check failed, e.g. `rows`. Stable, so it can be used to dedupe.
    pub kind: &'static str,
    /// `EXPLAIN` node type the check failed at (or `query`).
    pub node: String,
    /// Path from the root plus the compared values.
    pub detail: String,
}

impl Mismatch {
    /// Identifies a class of mismatch independently of the concrete query.
    pub fn signature(&self) -> String {
        format!("{} @ {}", self.kind, self.node)
    }
}

/// Result of comparing one execution.
#[derive(Debug, Default)]
pub struct Comparison {
    pub mismatches: Vec<Mismatch>,
    /// What the plan contained: `node:<EXPLAIN node type>` and `feature:<...>`.
    pub coverage: Vec<String>,
}

fn attr<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| &kv.value)
}

fn number(span: &SpanData, key: &str) -> Option<f64> {
    match attr(span, key)? {
        Value::F64(value) => Some(*value),
        Value::I64(value) => Some(*value as f64),
        _ => None,
    }
}

fn text(span: &SpanData, key: &str) -> Option<String> {
    attr(span, key).map(|value| value.as_str().into_owned())
}

fn flag(span: &SpanData, key: &str) -> bool {
    matches!(attr(span, key), Some(Value::Bool(true)))
}

/// The span `node_type` (`T_<Name>State`) for an `EXPLAIN` node type.
fn span_node_type(explain: &str) -> String {
    let name = match explain {
        "Aggregate" => "Agg",
        "Nested Loop" => "NestLoop",
        "Materialize" => "Material",
        "CTE Scan" => "CteScan",
        "Table Function Scan" => "TableFuncScan",
        other => return format!("T_{}State", other.replace(' ', "")),
    };
    format!("T_{name}State")
}

struct Walker<'a> {
    children: HashMap<SpanId, Vec<&'a SpanData>>,
    out: Comparison,
    /// `pg_otel.max_plan_spans`: only the nodes with a smaller pre-order
    /// index (the order EXPLAIN prints them in) have a span.
    max_spans: usize,
}

/// Number of nodes of the `EXPLAIN` plan tree `plan`.
fn tree_size(plan: &Json) -> usize {
    1 + plan["Plans"]
        .as_array()
        .map_or(0, |children| children.iter().map(tree_size).sum())
}

impl Walker<'_> {
    fn report(&mut self, kind: &'static str, node: &str, path: &str, detail: String) {
        self.out.mismatches.push(Mismatch {
            kind,
            node: node.to_owned(),
            detail: format!("{path}: {detail}"),
        });
    }

    /// `index` is the pre-order index of `plan`, below [`Walker::max_spans`].
    fn node(&mut self, plan: &Json, span: &SpanData, path: &str, index: usize) {
        let node_type = plan["Node Type"].as_str().unwrap_or("?").to_owned();
        let path = format!("{path}/{node_type}");
        self.out.coverage.push(format!("node:{node_type}"));

        let span_type = text(span, "postgresql.plan.node_type").unwrap_or_default();
        if span_node_type(&node_type) != span_type {
            self.report(
                "node_type",
                &node_type,
                &path,
                format!("EXPLAIN {node_type}, span {span_type}"),
            );
            // The subtrees cannot be compared meaningfully any more.
            return;
        }

        let loops = plan["Actual Loops"].as_f64().unwrap_or(f64::NAN);
        let span_loops = number(span, "postgresql.instrumentation.loops").unwrap_or(f64::NAN);
        if loops != span_loops {
            self.report(
                "loops",
                &node_type,
                &path,
                format!("EXPLAIN {loops}, span {span_loops}"),
            );
        }
        let loops_or_one = loops.max(1.0);

        // `EXPLAIN` prints per-loop averages rounded to 2 decimals (rows) or 3
        // decimals (ms); undo the division and allow the rounding error times
        // the number of loops.
        self.close(
            "rows",
            &node_type,
            &path,
            plan["Actual Rows"].as_f64().map(|v| v * loops),
            number(span, "postgresql.instrumentation.rows"),
            0.005 * loops_or_one,
        );
        self.close(
            "total_time",
            &node_type,
            &path,
            plan["Actual Total Time"].as_f64().map(|v| v * loops),
            number(span, "postgresql.instrumentation.total_time_seconds").map(|s| s * 1e3),
            0.0006 * loops_or_one,
        );
        self.close(
            "startup_time",
            &node_type,
            &path,
            plan["Actual Startup Time"].as_f64().map(|v| v * loops),
            number(span, "postgresql.instrumentation.startup_time_seconds").map(|s| s * 1e3),
            0.0006 * loops_or_one,
        );
        self.close(
            "plan_rows",
            &node_type,
            &path,
            plan["Plan Rows"].as_f64(),
            number(span, "postgresql.plan.rows"),
            0.5,
        );
        // Costs are printed with 2 decimals; huge ones also lose f64 precision.
        let cost_tolerance = |cost: Option<f64>| 0.006_f64.max(cost.unwrap_or(0.0).abs() * 1e-12);
        let (startup_cost, total_cost) =
            (plan["Startup Cost"].as_f64(), plan["Total Cost"].as_f64());
        self.close(
            "startup_cost",
            &node_type,
            &path,
            startup_cost,
            number(span, "postgresql.plan.startup_cost"),
            cost_tolerance(startup_cost),
        );
        self.close(
            "total_cost",
            &node_type,
            &path,
            total_cost,
            number(span, "postgresql.plan.total_cost"),
            cost_tolerance(total_cost),
        );
        self.close(
            "plan_width",
            &node_type,
            &path,
            plan["Plan Width"].as_f64(),
            number(span, "postgresql.plan.width_bytes"),
            0.0,
        );

        if (loops == 0.0) != flag(span, "postgresql.plan.never_executed") {
            self.report(
                "never_executed",
                &node_type,
                &path,
                format!("EXPLAIN loops {loops}"),
            );
        }
        if plan["Parallel Aware"].as_bool().unwrap_or(false)
            != flag(span, "postgresql.plan.parallel_aware")
        {
            self.report("parallel_aware", &node_type, &path, "differs".to_owned());
        }
        if plan["Async Capable"].as_bool().unwrap_or(false)
            != flag(span, "postgresql.plan.async_capable")
        {
            self.report("async_capable", &node_type, &path, "differs".to_owned());
        }
        // Both read the executor's own counters, which this harness never
        // leaves half-finished.
        if flag(span, "postgresql.plan.instrumentation_incomplete") {
            self.report(
                "instrumentation_incomplete",
                &node_type,
                &path,
                "set".to_owned(),
            );
        }
        // `ntuples2`, which EXPLAIN prints as a total (not per loop).
        let secondary = match node_type.as_str() {
            "Index Only Scan" => Some(("heap_fetches", "Heap Fetches")),
            "ModifyTable" if plan.get("Conflicting Tuples").is_some() => {
                Some(("conflicting_tuples", "Conflicting Tuples"))
            }
            _ => None,
        };
        if let Some((kind, key)) = secondary {
            self.close(
                kind,
                &node_type,
                &path,
                plan[key].as_f64(),
                number(span, "postgresql.instrumentation.secondary_rows"),
                0.5,
            );
        }

        self.equal(
            "parent_relationship",
            &node_type,
            &path,
            plan["Parent Relationship"].as_str().map(str::to_owned),
            text(span, "postgresql.plan.parent_relationship"),
        );
        self.equal(
            "subplan_name",
            &node_type,
            &path,
            plan["Subplan Name"].as_str().map(str::to_owned),
            text(span, "postgresql.plan.subplan_name"),
        );

        // `EXPLAIN` prints the bare table name, the span `schema.table`.
        self.equal(
            "relation",
            &node_type,
            &path,
            plan["Relation Name"].as_str().map(str::to_owned),
            text(span, "postgresql.plan.relation").map(|name| {
                name.rsplit_once('.')
                    .map_or(name.clone(), |(_, table)| table.to_owned())
            }),
        );

        self.close(
            "subplans_removed",
            &node_type,
            &path,
            Some(plan["Subplans Removed"].as_f64().unwrap_or(0.0)),
            Some(number(span, "postgresql.plan.subplans_removed").unwrap_or(0.0)),
            0.0,
        );
        self.equal(
            "workers_launched",
            &node_type,
            &path,
            plan["Workers Launched"].as_i64().map(|n| n.to_string()),
            number(span, "postgresql.plan.workers_launched").map(|n| (n as i64).to_string()),
        );

        // The filter counters are only compared when `EXPLAIN` printed them.
        let is_join = matches!(
            node_type.as_str(),
            "Nested Loop" | "Merge Join" | "Hash Join"
        );
        let (first, second) = if is_join {
            (
                plan.get("Rows Removed by Join Filter"),
                plan.get("Rows Removed by Filter"),
            )
        } else {
            (
                plan.get("Rows Removed by Filter"),
                plan.get("Rows Removed by Index Recheck"),
            )
        };
        for (kind, printed, key) in [
            (
                "filtered_first",
                first,
                "postgresql.instrumentation.rows_removed_by_scan_or_join_filter",
            ),
            (
                "filtered_other",
                second,
                "postgresql.instrumentation.rows_removed_by_other_filter",
            ),
        ] {
            if let Some(value) = printed.and_then(Json::as_f64) {
                self.close(
                    kind,
                    &node_type,
                    &path,
                    Some(value * loops),
                    number(span, key),
                    0.5 * loops_or_one,
                );
            }
        }

        if plan.get("Subplans Removed").is_some() {
            self.out
                .coverage
                .push("feature:subplans_removed".to_owned());
        }
        if plan["Workers Launched"].as_i64().is_some_and(|n| n > 0) {
            self.out
                .coverage
                .push("feature:workers_launched".to_owned());
        }
        if loops == 0.0 {
            self.out.coverage.push("feature:never_executed".to_owned());
        }
        if plan
            .get("Parent Relationship")
            .and_then(Json::as_str)
            .is_some_and(|r| r == "InitPlan" || r == "SubPlan")
        {
            self.out.coverage.push(format!(
                "feature:{}",
                plan["Parent Relationship"].as_str().unwrap_or("")
            ));
        }

        // The children that have a span: a prefix, being depth-first.
        let mut explain_children = Vec::new();
        let mut child_index = index + 1;
        for child in plan["Plans"].as_array().map_or(&[][..], Vec::as_slice) {
            if child_index >= self.max_spans {
                break;
            }
            explain_children.push((child, child_index));
            child_index += tree_size(child);
        }
        let span_children = self
            .children
            .get(&span.span_context.span_id())
            .cloned()
            .unwrap_or_default();
        if explain_children.len() != span_children.len() {
            let explain_names: Vec<_> = explain_children
                .iter()
                .map(|(c, _)| c["Node Type"].as_str().unwrap_or("?"))
                .collect();
            let span_names: Vec<_> = span_children
                .iter()
                .map(|c| text(c, "postgresql.plan.node_type").unwrap_or_default())
                .collect();
            self.report(
                "child_count",
                &node_type,
                &path,
                format!("EXPLAIN {explain_names:?}, spans {span_names:?}"),
            );
            return;
        }
        for ((child, child_index), child_span) in explain_children.into_iter().zip(span_children) {
            self.node(child, child_span, &path, child_index);
        }
    }

    /// Reports `kind` when the two numbers differ by more than `tolerance`.
    /// A side that is missing is skipped (the caller decides what is optional).
    fn close(
        &mut self,
        kind: &'static str,
        node: &str,
        path: &str,
        explain: Option<f64>,
        span: Option<f64>,
        tolerance: f64,
    ) {
        match (explain, span) {
            (Some(e), Some(s)) if (e - s).abs() <= tolerance + 1e-9 => {}
            (None, None) => {}
            (e, s) => self.report(kind, node, path, format!("EXPLAIN {e:?}, span {s:?}")),
        }
    }

    fn equal(
        &mut self,
        kind: &'static str,
        node: &str,
        path: &str,
        explain: Option<String>,
        span: Option<String>,
    ) {
        if explain != span {
            self.report(
                kind,
                node,
                path,
                format!("EXPLAIN {explain:?}, span {span:?}"),
            );
        }
    }
}

/// The shape of an `EXPLAIN` plan tree: node types with the properties that
/// make the executor (and so the plugin) take a different path, without
/// numbers or names. Two cases with the same shape exercise the same code.
pub fn plan_shape(plan: &Json) -> String {
    let mut out = String::new();
    shape_into(plan, &mut out);
    out
}

fn shape_into(plan: &Json, out: &mut String) {
    out.push_str(plan["Node Type"].as_str().unwrap_or("?"));
    for key in [
        "Parent Relationship",
        "Join Type",
        "Strategy",
        "Partial Mode",
        "Operation",
    ] {
        if let Some(value) = plan[key].as_str() {
            out.push(' ');
            out.push_str(value);
        }
    }
    if plan["Parallel Aware"].as_bool() == Some(true) {
        out.push_str(" parallel");
    }
    if plan["Actual Loops"].as_f64() == Some(0.0) {
        out.push_str(" never");
    }
    if plan["Subplans Removed"].as_f64().is_some_and(|n| n > 0.0) {
        out.push_str(" pruned");
    }
    if let Some(children) = plan["Plans"].as_array() {
        out.push('(');
        for (i, child) in children.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            shape_into(child, out);
        }
        out.push(')');
    }
}

/// Compares the `EXPLAIN` document (`[{"Plan": ..., "Execution Time": ...}]`)
/// with the spans captured while it ran with `pg_otel.max_plan_spans` =
/// `max_spans`. `may_reuse_plan`: the statement may run a cached plan, which
/// is not planned again and so has no planning span (EXPLAIN still prints a
/// planning time, of the plan cache lookup).
pub fn compare(
    explain: &Json,
    spans: &[SpanData],
    max_spans: usize,
    may_reuse_plan: bool,
) -> Comparison {
    let mut walker = Walker {
        children: HashMap::new(),
        out: Comparison::default(),
        max_spans,
    };
    let document = &explain[0];

    let queries: Vec<_> = spans
        .iter()
        .filter(|s| attr(s, "db.operation.name").is_some())
        .collect();
    let plan_spans: Vec<_> = spans
        .iter()
        .filter(|s| attr(s, "postgresql.plan.node_type").is_some())
        .collect();
    let [query] = queries[..] else {
        walker.report(
            "query_span",
            "query",
            "",
            format!("{} query spans", queries.len()),
        );
        return walker.out;
    };
    let plan_ids: Vec<SpanId> = plan_spans
        .iter()
        .map(|s| s.span_context.span_id())
        .collect();
    let mut roots = Vec::new();
    for span in &plan_spans {
        if plan_ids.contains(&span.parent_span_id) {
            walker
                .children
                .entry(span.parent_span_id)
                .or_default()
                .push(span);
        } else {
            roots.push(*span);
        }
    }
    let total = tree_size(&document["Plan"]);
    let omitted = total.saturating_sub(max_spans);
    walker.close(
        "spans_omitted",
        "query",
        "",
        (omitted > 0).then_some(omitted as f64),
        number(query, "postgresql.plan.spans_omitted"),
        0.0,
    );
    if (omitted > 0) != flag(query, "postgresql.plan.spans_truncated") {
        walker.report(
            "spans_truncated",
            "query",
            "",
            format!("{omitted} of {total} nodes omitted"),
        );
    }
    if omitted > 0 {
        walker
            .out
            .coverage
            .push("feature:spans_truncated".to_owned());
    }
    match roots[..] {
        [] if max_spans == 0 => {}
        [root] => walker.node(&document["Plan"], root, "", 0),
        _ => walker.report(
            "plan_roots",
            "query",
            "",
            format!("{} plan roots", roots.len()),
        ),
    }

    // pg_otel measures a part of what `EXPLAIN` measures, never more.
    let slack_ms = 0.001;
    if let (Some(span_ms), Some(explain_ms)) = (
        number(query, "postgresql.execution.total_time_seconds").map(|s| s * 1e3),
        document["Execution Time"].as_f64(),
    ) && span_ms > explain_ms + slack_ms
    {
        walker.report(
            "execution_time",
            "query",
            "",
            format!("span {span_ms} ms > EXPLAIN {explain_ms} ms"),
        );
    }
    let planning: Vec<_> = spans
        .iter()
        .filter(|s| text(s, "span.subtype").as_deref() == Some("planning"))
        .collect();
    match (planning[..].first(), document["Planning Time"].as_f64()) {
        (Some(span), Some(explain_ms)) if planning.len() == 1 => {
            let span_ms = number(span, "postgresql.planning.duration.us").unwrap_or(f64::NAN) / 1e3;
            if span_ms > explain_ms + slack_ms {
                walker.report(
                    "planning_time",
                    "query",
                    "",
                    format!("span {span_ms} ms > EXPLAIN {explain_ms} ms"),
                );
            }
        }
        (None, _) if may_reuse_plan => {}
        _ => walker.report(
            "planning_span",
            "query",
            "",
            format!("{} planning spans", planning.len()),
        ),
    }
    walker.out
}

#[cfg(test)]
mod tests {
    /// EXPLAIN prints huge estimates as exact integer expansions; they must
    /// parse to the double Postgres had (`serde_json` needs `float_roundtrip`
    /// for that), or the span's exact `plan_rows` looks one ULP off.
    #[test]
    fn explain_numbers_parse_to_the_exact_double() {
        // Plan Rows of a CI mismatch on PostgreSQL 18 and 19 (~1e23).
        for exact in [1.1667294954425583e23_f64, 1.1618397078605909e23_f64] {
            let printed = format!("{exact:.0}");
            let document: serde_json::Value = serde_json::from_str(&printed).unwrap();
            assert_eq!(document.as_f64(), Some(exact), "{printed}");
        }
    }
}
