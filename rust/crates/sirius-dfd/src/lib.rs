//! Exchange directives for distributed Sirius, from DataFusion Distributed's plan of a query.
//!
//! DataFusion plans the query, DataFusion Distributed (DFD) cuts that plan into stages, and
//! [`plan`] reads the cut back as the lines of a Sirius `exchange_directives` file (see
//! "Exchange directives" in `docs/super-sirius/distributed-sirius.md`). Nothing runs: no DFD
//! worker, no Arrow Flight. Sirius plans the query itself and follows the lines that name a site
//! of its own plan.

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{
    DistributedExec, DistributedExt, NetworkBoundaryExt, NetworkBroadcastExec,
    SessionStateBuilderExt, WorkerResolver, display_plan_ascii,
};
use std::sync::Arc;
use url::Url;

/// How to plan.
pub struct Options {
    /// The number of Sirius ranks: DFD's worker count.
    pub ranks: usize,
    /// DataFusion's partitions per task. With DFD's 16 MiB per partition, a table smaller than
    /// 16 MiB times this is read by one task. Fixed rather than DataFusion's default, the core
    /// count, so that every host plans alike.
    pub target_partitions: usize,
    /// Whether to write DFD's choice for each join as well. Without it Sirius keeps its own
    /// broadcast rule and only the grouped merges are directed.
    pub joins: bool,
    /// `key=value` settings of DataFusion or DFD, applied before planning.
    pub settings: Vec<String>,
}

/// What [`plan`] made of a query.
pub struct Planned {
    /// The text of an `exchange_directives` file.
    pub directives: String,
    /// DFD's plan, stage by stage; empty when DFD left the query on one task.
    pub stages: String,
}

/// One URL per Sirius rank. The planner only counts them.
struct Ranks(usize);

impl WorkerResolver for Ranks {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok((0..self.0).map(|r| Url::parse(&format!("sirius://rank/{r}")).unwrap()).collect())
    }
}

/// Plans `sql` over `tables` (name, Parquet directory) and returns its directives.
pub async fn plan(tables: &[(String, String)], sql: &str, options: &Options) -> Result<Planned> {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::new().with_target_partitions(options.target_partitions))
        .with_distributed_worker_resolver(Ranks(options.ranks))
        .with_distributed_planner()
        .build();
    let ctx = SessionContext::from(state);
    for (name, path) in tables {
        ctx.sql(&format!("CREATE EXTERNAL TABLE {name} STORED AS PARQUET LOCATION '{path}/'"))
            .await?
            .collect()
            .await?;
    }
    for setting in &options.settings {
        ctx.sql(&format!("SET {setting}")).await?.collect().await?;
    }
    let distributed =
        ctx.sql(sql.trim().trim_end_matches(';')).await?.create_physical_plan().await?;

    let mut directives = format!(
        "# sirius-dfd: ranks={} target_partitions={} joins={}\n",
        options.ranks,
        options.target_partitions,
        if options.joins { "dfd" } else { "sirius" }
    );
    let Some(exec) = distributed.downcast_ref::<DistributedExec>() else {
        directives += "# DataFusion Distributed left the query on one task: no directive.\n";
        return Ok(Planned { directives, stages: String::new() });
    };
    let mut lines = Vec::new();
    walk(exec.children()[0], Stage { number: 0, tasks: 1 }, options, &mut lines);
    for line in lines {
        directives += &line;
        directives.push('\n');
    }
    Ok(Planned { directives, stages: display_plan_ascii(distributed.as_ref(), false) })
}

#[derive(Clone, Copy)]
struct Stage {
    number: usize,
    tasks: usize,
}

/// The name of the column `expr` is, when it is one a directive can carry.
fn column(expr: &Arc<dyn PhysicalExpr>) -> Option<&str> {
    let name = expr.downcast_ref::<Column>()?.name();
    (!name.is_empty() && !name.contains(|c: char| c.is_whitespace() || ",=#".contains(c)))
        .then_some(name)
}

/// Whether `plan` is fed by a network broadcast, alone or under a coalesce.
fn is_broadcast(plan: &Arc<dyn ExecutionPlan>) -> bool {
    match plan.downcast_ref::<CoalescePartitionsExec>() {
        Some(coalesce) => is_broadcast(coalesce.input()),
        None => plan.is::<NetworkBroadcastExec>(),
    }
}

/// Appends the directives of the joins and aggregates of `plan`, which runs in `stage`.
fn walk(plan: &Arc<dyn ExecutionPlan>, stage: Stage, options: &Options, out: &mut Vec<String>) {
    if let Some(boundary) = plan.as_ref().as_network_boundary() {
        let input = boundary.input_stage();
        if let Some(head) = input.local_plan() {
            walk(head, Stage { number: input.num(), tasks: input.task_count() }, options, out);
        }
        return;
    }
    if let Some(join) = plan.downcast_ref::<HashJoinExec>().filter(|_| options.joins) {
        // The left side is the one DataFusion builds from.
        let keys: Option<Vec<String>> = join
            .on()
            .iter()
            .map(|(left, right)| Some(format!("{}={}", column(left)?, column(right)?)))
            .collect();
        // One task among several ranks: the join runs on rank 0 alone.
        let routing = if stage.tasks == 1 && options.ranks > 1 {
            Some("hash width=1")
        } else if *join.partition_mode() == PartitionMode::Partitioned {
            Some("hash")
        } else if is_broadcast(join.left()) {
            Some("broadcast")
        } else {
            None
        };
        if let (Some(keys), Some(routing)) = (keys, routing) {
            out.push(format!(
                "HASH_JOIN {} {routing}  # stage {}, {:?}",
                keys.join(","),
                stage.number,
                join.partition_mode()
            ));
        }
    }
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        let groups = aggregate.group_expr();
        let keys: Option<Vec<&str>> = groups.expr().iter().map(|(expr, _)| column(expr)).collect();
        // The final half of a split aggregate is fed through a hash boundary; an aggregate
        // DataFusion did not split runs where its input already is.
        let routing = match aggregate.mode() {
            AggregateMode::FinalPartitioned => Some("hash"),
            AggregateMode::SinglePartitioned => Some("none"),
            _ => None,
        };
        if let (Some(keys), Some(routing)) = (keys, routing) {
            if groups.is_single() && !keys.is_empty() && stage.tasks > 1 {
                out.push(format!(
                    "MERGE_GROUP_BY {} {routing}  # stage {}, {:?}",
                    keys.join(","),
                    stage.number,
                    aggregate.mode()
                ));
            }
        }
    }
    for child in plan.children() {
        walk(child, stage, options, out);
    }
}
