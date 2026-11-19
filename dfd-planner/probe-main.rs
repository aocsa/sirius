//! Calls DataFusion Distributed's public planner entry point on a finished DataFusion physical
//! plan and reads the boundaries back through the public API only, as the exchanges a Sirius rank
//! would insert (`exchange_spec` in src/op/sirius_physical_exchange_sink.hpp). Nothing is executed:
//! no worker, no Arrow Flight, no gRPC.
//!
//!   dfd-probe DATA_DIR QUERY.sql RANKS TARGET_PARTITIONS [--plan] [--set KEY=VALUE]...
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{
    DistributedExec, DistributedExt, NetworkBoundaryExt, NetworkBroadcastExec,
    NetworkCoalesceExec, NetworkShuffleExec, SessionStateBuilderExt, WorkerResolver,
    create_distributed_plan, display_plan_ascii,
};
use std::sync::Arc;
use url::Url;

/// One URL per Sirius rank. The planner only counts them.
struct Ranks(usize);

impl WorkerResolver for Ranks {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok((0..self.0).map(|r| Url::parse(&format!("sirius://rank/{r}")).unwrap()).collect())
    }
}

const TABLES: [&str; 8] =
    ["lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"];

/// Prints one line per boundary under `plan`, which runs in a stage of `consumers` tasks.
fn walk(plan: &Arc<dyn ExecutionPlan>, consumers: usize, consumer_stage: usize, out: &mut Vec<String>) {
    let Some(boundary) = plan.as_ref().as_network_boundary() else {
        for child in plan.children() {
            walk(child, consumers, consumer_stage, out);
        }
        return;
    };
    let stage = boundary.input_stage();
    let producers = stage.task_count();
    let head = stage.local_plan().expect("a planned stage holds its plan");
    let mode = if plan.is::<NetworkShuffleExec>() {
        "hash"
    } else if plan.is::<NetworkBroadcastExec>() {
        "broadcast"
    } else if plan.is::<NetworkCoalesceExec>() {
        "gather"
    } else {
        "?"
    };
    let mut keys = String::new();
    if mode == "hash" {
        let repartition = head.downcast_ref::<RepartitionExec>().expect("a shuffle stage ends in a RepartitionExec");
        let Partitioning::Hash(exprs, buckets) = repartition.partitioning() else {
            panic!("a shuffle stage's head is hash partitioned")
        };
        // A two-step shuffle appends a salt literal to the keys (execution_plans/common.rs).
        let salted = exprs.last().is_some_and(|e| e.is::<Literal>());
        let columns: Vec<String> = exprs[..exprs.len() - usize::from(salted)]
            .iter()
            .map(|e| match e.downcast_ref::<Column>() {
                Some(c) => format!("{}:{}", c.index(), c.name()),
                None => format!("<{e}>"),
            })
            .collect();
        keys = format!(
            " key_columns=[{}] buckets={buckets}{}",
            columns.join(", "),
            if salted { " salted" } else { "" }
        );
    }
    // exchange_spec::width: only ranks 0..width-1 receive; 0 = every rank.
    let spec = match mode {
        "gather" => "target=0".to_string(),
        _ => format!("width={consumers}"),
    };
    out.push(format!(
        "stage {:>2} -> stage {:>2}  exchange_spec{{routing={mode}{keys} {spec}}}  senders={producers} below={}",
        stage.num(),
        consumer_stage,
        head.children().first().map_or(head.name(), |c| c.name()),
    ));
    walk(head, producers, stage.num(), out);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let (data, query, ranks, partitions) = (&args[1], &args[2], args[3].parse()?, args[4].parse()?);
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::new().with_target_partitions(partitions))
        .with_distributed_worker_resolver(Ranks(ranks))
        // Registers DFD's built-in file-scan task-count handlers and its config. The query planner
        // it installs is not used below.
        .with_distributed_planner()
        .build();
    let ctx = SessionContext::from(state);
    for t in TABLES {
        ctx.sql(&format!("CREATE EXTERNAL TABLE {t} STORED AS PARQUET LOCATION '{data}/{t}/'"))
            .await?
            .collect()
            .await?;
    }
    for (i, arg) in args.iter().enumerate() {
        if arg == "--set" {
            ctx.sql(&format!("SET {}", args[i + 1])).await?.collect().await?;
        }
    }
    let sql = std::fs::read_to_string(query)?;
    let (state, logical) = ctx.sql(sql.trim().trim_end_matches(';')).await?.into_parts();
    let logical = state.optimize(&logical)?;
    // DataFusion's own physical plan, as any DataFusion embedder gets it.
    let single = DefaultPhysicalPlanner::default().create_physical_plan(&logical, &state).await?;
    // The public entry point.
    let distributed = create_distributed_plan(Arc::clone(&single), &state).await?;

    let Some(exec) = distributed.downcast_ref::<DistributedExec>() else {
        println!("not distributed: create_distributed_plan returned the single-node plan");
        return Ok(());
    };
    let mut lines = Vec::new();
    walk(exec.children()[0], 1, 0, &mut lines);
    println!("{query}: ranks={ranks} target_partitions={partitions} boundaries={}", lines.len());
    for line in &lines {
        println!("  {line}");
    }
    if args.iter().any(|a| a == "--plan") {
        println!("{}", display_plan_ascii(distributed.as_ref(), false));
    }
    Ok(())
}
