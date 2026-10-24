//! The sources a build reads through. Every source is constructed with the
//! dataset schema (`with_schema`, `docs/format.md`: the single source of
//! column types) and the index's fingerprint policy. Text files are read at
//! the chunk size the manifest records for them, so there is one text
//! source per distinct chunk size.

use std::collections::BTreeMap;
use std::ops::Range;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bellwether_source::{
    ChunkedTextSource, FingerprintPolicy, LocalFs, ParquetSource, PathTemplate, TextFormat,
};
use bellwether_types::{Error, FileFingerprint, Result, RowGroupMeta, Source};

use crate::config::SourceKind;

/// A file's row groups as needed to assign ids: how many, and for text
/// files the byte range of each chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileLayout {
    pub(crate) row_groups: usize,
    pub(crate) chunks: Option<Vec<Range<u64>>>,
}

/// The source of one index: Parquet, or text at one or more chunk sizes.
#[derive(Debug)]
pub(crate) enum DataSource {
    Parquet(ParquetSource<LocalFs>),
    Text {
        format: TextFormat,
        schema: SchemaRef,
        policy: FingerprintPolicy,
        path_template: Option<PathTemplate>,
        by_chunk_size: BTreeMap<u64, ChunkedTextSource<LocalFs>>,
    },
}

impl DataSource {
    /// Sources for `kind` under the dataset `schema` and `policy`. For text,
    /// one source per chunk size in `chunk_sizes`.
    pub(crate) fn new(
        kind: SourceKind,
        schema: &SchemaRef,
        policy: FingerprintPolicy,
        path_template: Option<PathTemplate>,
        chunk_sizes: impl IntoIterator<Item = u64>,
    ) -> Self {
        match kind.text_format() {
            None => {
                let mut p = ParquetSource::new()
                    .with_schema(SchemaRef::clone(schema))
                    .with_fingerprint_policy(policy);
                if let Some(tpl) = path_template {
                    p = p.with_path_template(tpl);
                }
                Self::Parquet(p)
            }
            Some(format) => {
                let mut by_chunk_size = BTreeMap::new();
                for size in chunk_sizes {
                    by_chunk_size.entry(size).or_insert_with(|| {
                        text_source(format, schema, policy, path_template.clone(), size)
                    });
                }
                Self::Text {
                    format,
                    schema: SchemaRef::clone(schema),
                    policy,
                    path_template,
                    by_chunk_size,
                }
            }
        }
    }

    /// Adds a text source for `size` if there is none yet.
    pub(crate) fn ensure_chunk_size(&mut self, size: u64) {
        if let Self::Text {
            format,
            schema,
            policy,
            path_template,
            by_chunk_size,
        } = self
        {
            by_chunk_size.entry(size).or_insert_with(|| {
                text_source(*format, schema, *policy, path_template.clone(), size)
            });
        }
    }

    fn text(&self, chunk_size: Option<u64>) -> Result<&ChunkedTextSource<LocalFs>> {
        let Self::Text { by_chunk_size, .. } = self else {
            return Err(Error::Other("not a text source".into()));
        };
        let size =
            chunk_size.ok_or_else(|| Error::Other("a text file entry has no chunk size".into()))?;
        by_chunk_size
            .get(&size)
            .ok_or_else(|| Error::Other(format!("no text source for chunk size {size}")))
    }

    fn source(&self, chunk_size: Option<u64>) -> Result<&dyn Source> {
        match self {
            Self::Parquet(p) => {
                if chunk_size.is_some() {
                    return Err(Error::Other(
                        "a Parquet file entry records a chunk size".into(),
                    ));
                }
                Ok(p)
            }
            Self::Text { .. } => Ok(self.text(chunk_size)?),
        }
    }

    /// Files under `root` with their fingerprints, sorted by path.
    pub(crate) fn discover(&self, root: &str) -> Result<Vec<FileFingerprint>> {
        let mut files = match self {
            Self::Parquet(p) => p.discover(root)?,
            Self::Text { by_chunk_size, .. } => match by_chunk_size.values().next() {
                Some(t) => t.discover(root)?,
                None => return Err(Error::Other("no text source configured".into())),
            },
        };
        files.sort_by(|a, b| a.path.cmp(&b.path));
        if let Some(w) = files.windows(2).find(|w| w[0].path == w[1].path) {
            return Err(Error::Source(format!("{} was discovered twice", w[0].path)));
        }
        Ok(files)
    }

    /// The row-group count of `file` and, for text, its chunk ranges at
    /// `chunk_size`. Verifies the fingerprint on the opened file.
    pub(crate) fn layout(
        &self,
        file: &FileFingerprint,
        chunk_size: Option<u64>,
    ) -> Result<FileLayout> {
        match self {
            Self::Parquet(p) => {
                if chunk_size.is_some() {
                    return Err(Error::Other(
                        "a Parquet file cannot have a chunk size".into(),
                    ));
                }
                let footer = p.footer(file)?;
                Ok(FileLayout {
                    row_groups: footer.metadata().num_row_groups(),
                    chunks: None,
                })
            }
            Self::Text { .. } => {
                let chunks = self.text(chunk_size)?.chunk_ranges(file)?;
                Ok(FileLayout {
                    row_groups: chunks.len(),
                    chunks: Some(chunks),
                })
            }
        }
    }

    /// `Source::row_groups` of `file`, read at `chunk_size` for text.
    pub(crate) fn row_groups(
        &self,
        file: &FileFingerprint,
        chunk_size: Option<u64>,
    ) -> Result<Vec<RowGroupMeta>> {
        self.source(chunk_size)?.row_groups(file)
    }

    /// `Source::read_columns` of one row group, read at `chunk_size` for
    /// text.
    pub(crate) fn read_columns(
        &self,
        rg: &RowGroupMeta,
        columns: &[String],
        chunk_size: Option<u64>,
    ) -> Result<RecordBatch> {
        self.source(chunk_size)?.read_columns(rg, columns)
    }
}

fn text_source(
    format: TextFormat,
    schema: &SchemaRef,
    policy: FingerprintPolicy,
    path_template: Option<PathTemplate>,
    chunk_size: u64,
) -> ChunkedTextSource<LocalFs> {
    let mut s = ChunkedTextSource::new(format)
        .with_chunk_size(chunk_size)
        .with_schema(SchemaRef::clone(schema))
        .with_fingerprint_policy(policy);
    if let Some(tpl) = path_template {
        s = s.with_path_template(tpl);
    }
    s
}

/// The schema of the files under `root` as the source infers it: the first
/// Parquet footer, or the inferred head of the first text file. Used only
/// for a new index built without a supplied schema.
pub(crate) fn infer_schema(
    kind: SourceKind,
    root: &str,
    policy: FingerprintPolicy,
    path_template: Option<PathTemplate>,
) -> Result<SchemaRef> {
    match kind.text_format() {
        None => {
            let mut s = ParquetSource::new().with_fingerprint_policy(policy);
            if let Some(tpl) = path_template {
                s = s.with_path_template(tpl);
            }
            s.schema(root)
        }
        Some(format) => {
            let mut s = ChunkedTextSource::new(format).with_fingerprint_policy(policy);
            if let Some(tpl) = path_template {
                s = s.with_path_template(tpl);
            }
            s.schema(root)
        }
    }
}
