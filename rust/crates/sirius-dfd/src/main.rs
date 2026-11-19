//! Writes the `exchange_directives` file of a query for distributed Sirius.
//!
//!   sirius-dfd --data DIR | --table NAME=DIR ...  --ranks N  [--target-partitions P]
//!              [--joins] [--set KEY=VALUE]... [--stages] [-o FILE] QUERY.sql
//!
//! `--data DIR` registers every subdirectory of DIR as a Parquet table of that name.
use sirius_dfd::{Options, plan};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mut tables = Vec::new();
    let mut options = Options { ranks: 0, target_partitions: 16, joins: false, settings: vec![] };
    let (mut query, mut output, mut stages) = (None, None, false);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--data" => {
                let dir = value()?;
                let mut names = Vec::new();
                for entry in std::fs::read_dir(&dir)? {
                    let entry = entry?;
                    if entry.file_type()?.is_dir() {
                        names.push(entry.file_name().to_string_lossy().into_owned());
                    }
                }
                names.sort();
                tables.extend(names.into_iter().map(|name| {
                    let path = format!("{dir}/{name}");
                    (name, path)
                }));
            }
            "--table" => {
                let table = value()?;
                let (name, path) = table.split_once('=').ok_or("--table takes NAME=DIR")?;
                tables.push((name.to_string(), path.to_string()));
            }
            "--ranks" => options.ranks = value()?.parse()?,
            "--target-partitions" => options.target_partitions = value()?.parse()?,
            "--set" => options.settings.push(value()?),
            "--joins" => options.joins = true,
            "--stages" => stages = true,
            "-o" => output = Some(value()?),
            _ => query = Some(arg),
        }
    }
    let query = query.ok_or("no query file")?;
    if options.ranks == 0 || tables.is_empty() {
        return Err("--ranks and --data or --table are required".into());
    }
    let planned = plan(&tables, &std::fs::read_to_string(&query)?, &options).await?;
    if stages {
        eprintln!("{}", planned.stages);
    }
    let text = format!("# {query}\n{}", planned.directives);
    match output {
        Some(path) => std::fs::write(path, text)?,
        None => print!("{text}"),
    }
    Ok(())
}
