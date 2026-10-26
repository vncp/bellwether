//! Text keys (B5) in the facade: the text lookup `locate` and `why` take,
//! how `why` records text lookups as workload runs, the text key's `why`
//! column (distinct tokens, and when the column is too high-entropy for the
//! key to pay), and what the `R-text-1` detector reads from a workload.
//!
//! A text lookup is an ordinary workload run (`has_token`, `contains`, or
//! `has_prefix`, the replay crate's `RunQuery::Text`): its key set is the
//! words, substrings, or prefixes over its one string column, so a text key
//! is proposed, replayed, verified, and receipted as every other key is.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use bellwether_core::text::TokenStats;
use bellwether_profile::lints::TextUse;
use bellwether_replay::{KeyInput, Replayer, RunQuery, Workload, WorkloadRecorder};
use bellwether_types::{Error, Exactness, KeySpec, KindColumn, Result, TextOp, Tokenizer};
use serde::{Deserialize, Serialize};

/// One text lookup (B5): rows whose string `column` matches `value` under
/// `op`, both sides case-folded.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TextQuery {
    /// The string column.
    pub column: String,
    /// The match.
    pub op: TextOp,
    /// The word, substring, or prefix, as given.
    pub value: String,
}

/// A text key holding at least this many distinct tokens per row is
/// high-entropy: most tokens occur once, so its dictionary costs about as
/// much as the column, and a token prunes to the few row groups that hold
/// it only by spending that size.
pub const HIGH_ENTROPY_TOKENS_PER_ROW: f64 = 0.5;

/// Why a key over a column with these counts is too high-entropy to pay,
/// or `None`: at least [`HIGH_ENTROPY_TOKENS_PER_ROW`] distinct tokens
/// (words or trigrams) per row.
#[must_use]
pub fn high_entropy(tokenizer: Tokenizer, tokens: &TokenStats, rows: u64) -> Option<String> {
    #[allow(clippy::cast_precision_loss)]
    let high = rows > 0 && tokens.distinct as f64 >= HIGH_ENTROPY_TOKENS_PER_ROW * rows as f64;
    high.then(|| {
        format!(
            "high entropy: {} distinct {} over {rows} rows, so most occur once and the \
             dictionary is about as large as the column",
            tokens.distinct,
            match tokenizer {
                Tokenizer::Words => "words",
                Tokenizer::Trigram => "trigrams",
            }
        )
    })
}

/// A text key's `why` column: the distinct tokens (words or trigrams) of
/// its column, exact, counted over the transient index the candidate was
/// priced with (a token in several segments counts once). The label says
/// which tokens; for trigrams, how many row groups the median trigram is
/// in (a substring prunes only by its rarer trigrams); when the column is
/// too high-entropy for the key ([`high_entropy`]), why; and when the
/// replay shows the key reads no fewer bytes on the runs (`pays` false),
/// that.
pub(crate) fn kind_column(
    replayer: &Replayer<'_>,
    spec: &KeySpec,
    pays: bool,
) -> Result<KindColumn> {
    let KeySpec::Text { tokenizer, .. } = spec else {
        return Err(Error::Other(format!("{} is not a text key", spec.name())));
    };
    let tokens = replayer.text_stats(spec)?;
    let catalog = replayer.catalog();
    Ok(KindColumn {
        kind: "text".into(),
        label: kind_label(
            *tokenizer,
            &tokens,
            catalog.rows_total(),
            catalog.len() as u64,
            pays,
        ),
        value: tokens.distinct.to_string(),
        exactness: Exactness::Exact,
    })
}

/// The label of a text key's `why` column ([`kind_column`]).
fn kind_label(
    tokenizer: Tokenizer,
    tokens: &TokenStats,
    rows: u64,
    row_groups: u64,
    pays: bool,
) -> String {
    let mut label = match tokenizer {
        Tokenizer::Words => "distinct words".to_string(),
        Tokenizer::Trigram => format!(
            "distinct trigrams; the median trigram is in {} of {row_groups} row groups, so a \
             substring prunes only by its rarer trigrams",
            tokens.row_groups_per_token[0]
        ),
    };
    if let Some(why) = high_entropy(tokenizer, tokens, rows) {
        label.push_str("; ");
        label.push_str(&why);
    }
    if !pays {
        label.push_str("; reads no fewer bytes than the current configuration on these runs");
    }
    label
}

/// The text keys a text workload's floor is, per column in first-seen
/// order: a words dictionary when some run on the column asks for whole
/// words or word prefixes (it plans only row groups holding a match), then
/// a trigram key, which serves every text match (with candidates).
pub(crate) fn floor_keys(workload: &Workload) -> Result<Vec<KeySpec>> {
    let mut out: Vec<KeySpec> = Vec::new();
    for u in text_uses(workload)? {
        if u.has_token + u.has_prefix > 0 {
            out.push(KeySpec::Text {
                column: u.column.clone(),
                tokenizer: Tokenizer::Words,
                bloom: None,
            });
        }
        out.push(KeySpec::Text {
            column: u.column,
            tokenizer: Tokenizer::Trigram,
            bloom: None,
        });
    }
    Ok(out)
}

/// What the `R-text-1` detector reads from a workload: per string column
/// its text runs look up, in first-seen order, the runs per match.
pub(crate) fn text_uses(workload: &Workload) -> Result<Vec<TextUse>> {
    let mut order: Vec<String> = Vec::new();
    let mut uses: BTreeMap<String, TextUse> = BTreeMap::new();
    for r in workload.runs() {
        let (Some(op), [column]) = (r.query()?.text_op(), r.columns.as_slice()) else {
            continue;
        };
        let u = uses.entry(column.clone()).or_insert_with(|| {
            order.push(column.clone());
            TextUse {
                column: column.clone(),
                ..TextUse::default()
            }
        });
        match op {
            TextOp::Contains => u.contains += 1,
            TextOp::HasToken => u.has_token += 1,
            TextOp::HasPrefix => u.has_prefix += 1,
        }
    }
    Ok(order.iter().filter_map(|c| uses.remove(c)).collect())
}

/// Records each lookup of `queries` as one text run (`text-1`, `text-2`,
/// ...), its key set the one value over its column at the dataset's type.
pub(crate) fn record(
    recorder: &mut WorkloadRecorder<'_>,
    schema: &Schema,
    queries: &[TextQuery],
) -> Result<()> {
    for (i, q) in queries.iter().enumerate() {
        let field = schema.field_with_name(&q.column).map_err(|_| {
            Error::KeySpec(format!(
                "column {:?} is not in the dataset; a text lookup names a string column",
                q.column
            ))
        })?;
        bellwether_core::text::check_column_type(&q.column, field.data_type())?;
        let values: ArrayRef = Arc::new(StringArray::from(vec![q.value.as_str()]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                &q.column,
                arrow::datatypes::DataType::Utf8,
                false,
            )])),
            vec![values],
        )
        .map_err(|e| Error::Other(e.to_string()))?;
        recorder.record_query(
            &format!("text-{}", i + 1),
            KeyInput::Batch(&batch),
            RunQuery::Text(q.op),
            None,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(distinct: u64, p50: u64, max: u64) -> TokenStats {
        TokenStats {
            distinct,
            row_groups_per_token: [p50, max, max],
        }
    }

    /// The rebuild step a text segment refusal carries names the data
    /// root's `bellwether index` command, and nothing about moving the
    /// index (the ledger lives there).
    #[test]
    fn the_rebuild_step_names_index_on_the_root() {
        let e = Error::KeySpec(format!(
            "this text:body,tokenizer=words segment was built with other tables; {}",
            bellwether_core::text::REBUILD_STEP
        ));
        let m = crate::engine::root_hint("/data/my lake", e).to_string();
        assert!(m.contains("run `bellwether index '/data/my lake'`"), "{m}");
        assert!(m.contains("keeps the ledger"), "{m}");
        assert!(!m.contains("aside"), "{m}");
    }

    /// High entropy at one distinct token per two rows, not below.
    #[test]
    fn high_entropy_is_one_distinct_token_per_two_rows() {
        assert!(high_entropy(Tokenizer::Words, &stats(50, 1, 1), 100).is_some());
        assert!(high_entropy(Tokenizer::Words, &stats(49, 1, 1), 100).is_none());
        assert!(high_entropy(Tokenizer::Trigram, &stats(60, 1, 1), 100)
            .is_some_and(|m| m.contains("60 distinct trigrams over 100 rows")));
        assert!(high_entropy(Tokenizer::Words, &stats(0, 0, 0), 0).is_none());
    }

    /// The label says which tokens, the median trigram's row groups (not
    /// the max), high entropy when it holds, and when the key reads no
    /// fewer bytes.
    #[test]
    fn the_label_states_each_verdict() {
        let l = kind_label(Tokenizer::Trigram, &stats(10, 2, 9), 100, 10, true);
        assert!(
            l.starts_with("distinct trigrams; the median trigram is in 2 of 10 row groups"),
            "{l}"
        );
        assert!(
            !l.contains("high entropy") && !l.contains("no fewer bytes"),
            "{l}"
        );
        let l = kind_label(Tokenizer::Words, &stats(80, 1, 1), 100, 10, false);
        assert!(
            l.starts_with("distinct words; high entropy: 80 distinct words"),
            "{l}"
        );
        assert!(
            l.ends_with("reads no fewer bytes than the current configuration on these runs"),
            "{l}"
        );
        let l = kind_label(Tokenizer::Words, &stats(10, 1, 1), 100, 10, true);
        assert_eq!(l, "distinct words");
    }
}
