use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use sirius_dfd::{Options, plan};

/// Two tables large enough in rows to join, with settings that make DataFusion Distributed give
/// their small files a task per rank and a partitioned join.
async fn planned(sql: &str, joins: bool) -> String {
    let dir = tempfile::tempdir().unwrap();
    let ctx = SessionContext::new();
    let mut tables = Vec::new();
    for (name, select) in [
        ("fact", "SELECT value AS fk, value % 7 AS fv FROM generate_series(1, 100000)"),
        ("dim", "SELECT value AS dk, value % 5 AS dv FROM generate_series(1, 60000)"),
    ] {
        let path = dir.path().join(name);
        std::fs::create_dir(&path).unwrap();
        ctx.sql(select)
            .await
            .unwrap()
            .write_parquet(
                path.join("part-0.parquet").to_str().unwrap(),
                DataFrameWriteOptions::new(),
                None,
            )
            .await
            .unwrap();
        tables.push((name.to_string(), path.to_str().unwrap().to_string()));
    }
    let options = Options {
        ranks: 4,
        target_partitions: 4,
        joins,
        settings: vec![
            "distributed.file_scan_config_bytes_per_partition=1".to_string(),
            "datafusion.optimizer.hash_join_single_partition_threshold=0".to_string(),
            "datafusion.optimizer.hash_join_single_partition_threshold_rows=0".to_string(),
        ],
    };
    plan(&tables, sql, &options).await.unwrap().directives
}

fn lines(directives: &str) -> Vec<String> {
    directives
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| line.split('#').next().unwrap().trim().to_string())
        .collect()
}

#[tokio::test]
async fn an_aggregate_on_the_join_key_needs_no_exchange() {
    let sql = "SELECT fk, count(*) FROM fact JOIN dim ON fk = dk GROUP BY fk";
    assert_eq!(lines(&planned(sql, false).await), ["MERGE_GROUP_BY fk none"]);

    let with_joins = lines(&planned(sql, true).await);
    assert_eq!(with_joins.len(), 2, "{with_joins:?}");
    assert!(with_joins.contains(&"MERGE_GROUP_BY fk none".to_string()));
    assert!(
        with_joins.contains(&"HASH_JOIN fk=dk hash".to_string())
            || with_joins.contains(&"HASH_JOIN dk=fk hash".to_string()),
        "{with_joins:?}"
    );
}

#[tokio::test]
async fn an_aggregate_on_another_column_is_hashed() {
    let sql = "SELECT fv, count(*) FROM fact JOIN dim ON fk = dk GROUP BY fv";
    assert_eq!(lines(&planned(sql, false).await), ["MERGE_GROUP_BY fv hash"]);
}
