//! What an engine adapter needs beside a plan (WP16, `--emit`): the
//! dataset schema the plan was computed against, the row counts of the
//! planned files' row groups, and their partition values, all read from
//! the manifest at the plan's own sequence. Adapters never read a
//! manifest; the facade hands them this.

use std::collections::BTreeMap;

use arrow::datatypes::SchemaRef;
use bellwether_source::Partitioning;
use bellwether_types::{Error, Result, ScanPlan};

use crate::engine::Engine;

/// The planned files' partition values (`hive` or a path template).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmitPartitions {
    /// The recorded scheme, as the manifest spells it.
    pub scheme: String,
    /// The partition columns, in path order.
    pub columns: Vec<String>,
    /// Per planned file (by the plan's path), one value per column
    /// (`None` for Hive's null).
    pub files: BTreeMap<String, Vec<Option<String>>>,
}

/// The inputs an adapter takes from the manifest a plan was computed
/// against.
#[derive(Clone, Debug)]
pub struct EmitInputs {
    /// The dataset schema, partition columns included.
    pub schema: SchemaRef,
    /// Rows of every row group of each planned file, in `index_in_file`
    /// order; `None` when the manifest does not record them (storage
    /// version 1), and the adapter then reads them from the footers.
    pub row_counts: Option<BTreeMap<String, Vec<u64>>>,
    /// The partition values, for a partitioned dataset.
    pub partitions: Option<EmitPartitions>,
}

impl Engine {
    /// The schema, row counts, and partition values of `plan`'s files, from
    /// the manifest at the plan's sequence (`plan.manifest_version`). A
    /// plan over a table or a text source has no Parquet files to hand an
    /// engine and is refused.
    pub fn emit_inputs(&self, plan: &ScanPlan) -> Result<EmitInputs> {
        let seq: u64 = plan.manifest_version.parse().map_err(|_| {
            Error::Other(format!(
                "the plan's manifest version {:?} is not a sequence; emit a plan this index \
                 computed",
                plan.manifest_version
            ))
        })?;
        let pinned = self.pin_sequence(seq)?.ok_or_else(|| Error::NotIndexed {
            root: self.root().to_string(),
            operation: "emit",
            needs: None,
        })?;
        let m = pinned.manifest();
        if !m.source.kind.is_empty() && m.source.kind != "parquet" {
            return Err(Error::Other(format!(
                "--emit hands Parquet files to an engine, and this index is over a {} source; \
                 query it through bellwether instead",
                m.source.kind
            )));
        }
        let planned: std::collections::BTreeSet<&str> =
            plan.entries.iter().map(|e| e.file.path.as_str()).collect();
        let mut rows = BTreeMap::new();
        let mut complete = true;
        for f in &m.rowgroup_space.files {
            if !planned.contains(f.fingerprint.path.as_str()) {
                continue;
            }
            match &f.rowgroup_rows {
                Some(r) => {
                    rows.insert(f.fingerprint.path.clone(), r.clone());
                }
                None => complete = false,
            }
        }
        let partitioning = Partitioning::parse(m.source.partitioning.as_deref())?;
        let partitions = match partitioning.manifest_name() {
            None => None,
            Some(scheme) => {
                let mut columns: Option<Vec<String>> = None;
                let mut files = BTreeMap::new();
                for e in &plan.entries {
                    let values = partitioning.values(self.root(), &e.file.path)?;
                    let names: Vec<String> = values.iter().map(|v| v.name.clone()).collect();
                    if columns.get_or_insert_with(|| names.clone()) != &names {
                        return Err(Error::Other(format!(
                            "{} has partition columns {names:?}, unlike the other planned files; \
                             run `bellwether index` on the folder",
                            e.file.path
                        )));
                    }
                    files.insert(
                        e.file.path.clone(),
                        values.into_iter().map(|v| v.value).collect(),
                    );
                }
                let columns = match columns {
                    Some(c) => c,
                    None => partitioning.column_names(self.root()).unwrap_or_default(),
                };
                Some(EmitPartitions {
                    scheme,
                    columns,
                    files,
                })
            }
        };
        Ok(EmitInputs {
            schema: SchemaRef::clone(pinned.planner.schema()),
            row_counts: complete.then_some(rows),
            partitions,
        })
    }
}
