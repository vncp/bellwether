//! `keys`: signing keys and the trust file (ADR 029), and the flags that
//! hand them to `fix` and to the commands that read receipts.

use std::fmt::Write as _;
use std::path::PathBuf;

use bellwether::types::Error;
use bellwether::{ChainLink, ReceiptResult, RecordKind, SignatureStatus, TrustedKeys};
use clap::{Args, Subcommand};

use crate::human::short;
use crate::{done, engine_keyed, hash, human, Failure, Global, Out, Outcome, EXIT_FAILED, EXIT_OK};

/// `--trusted-keys`: the reader's trust file.
#[derive(Args)]
pub(crate) struct TrustArg {
    /// The trust file `bellwether keys trust` writes: receipts and
    /// certificates signed by one of its keys, within the key's window,
    /// are reported as signed.
    #[arg(long, value_name = "FILE", env = "BELLWETHER_TRUSTED_KEYS")]
    pub(crate) trusted_keys: Option<PathBuf>,
}

impl TrustArg {
    /// The trust file, read now; a file that cannot be used is a usage
    /// error naming it.
    pub(crate) fn load(&self) -> Result<Option<TrustedKeys>, Failure> {
        self.trusted_keys
            .as_deref()
            .map(bellwether::load_trusted_keys)
            .transpose()
            .map_err(Failure::from)
    }
}

/// `keys` subcommands.
#[derive(Subcommand)]
pub(crate) enum KeysCommand {
    /// Generate an ed25519 signing key: the 32-byte seed at --out (mode
    /// 0600, never overwritten) and its public half at <out>.pub.
    Generate {
        /// Where to write the seed; the public half goes beside it.
        #[arg(long, value_name = "PATH")]
        out: PathBuf,
        /// A label for people, which verdicts name the signer by [default:
        /// the file name of --out].
        #[arg(long, value_name = "LABEL")]
        name: Option<String>,
    },
    /// Print the key id of a public key file.
    Show {
        /// The public key file (<key>.pub).
        public: PathBuf,
    },
    /// Add a public key to a trust file (created if absent).
    Trust {
        /// The trust file.
        #[arg(long, value_name = "FILE")]
        file: PathBuf,
        /// The public key file (<key>.pub) to trust.
        #[arg(long, value_name = "PUB")]
        add: PathBuf,
        /// Signatures made before this instant do not count (RFC 3339).
        #[arg(long, value_name = "RFC3339")]
        valid_from: Option<String>,
        /// Signatures made at or after this instant do not count (RFC
        /// 3339): the key's revocation.
        #[arg(long, value_name = "RFC3339")]
        revoked_at: Option<String>,
    },
}

pub(crate) fn run(g: &Global, cmd: &KeysCommand) -> Result<Outcome, Failure> {
    match cmd {
        KeysCommand::Generate { out: path, name } => {
            let r = bellwether::generate_key(path, name.as_deref(), g.engine.clock.as_deref())?;
            let mut s = String::new();
            let _ = writeln!(
                s,
                "wrote the signing key {} (readable by you only) and its public half {}",
                r.path, r.public_path
            );
            let _ = writeln!(s, "  key id  {}", r.public.key_id);
            let _ = writeln!(s, "  name    {}", r.public.name);
            let _ = writeln!(
                s,
                "keep {} private and pass it to `bellwether fix --signing-key`; readers trust \
                 the key with `bellwether keys trust --file <trust file> --add {}`",
                r.path, r.public_path
            );
            done(&r, s, EXIT_OK)
        }
        KeysCommand::Show { public } => {
            let p = bellwether::read_public_key(public)?;
            let s = format!(
                "key id {}\n  name        {}\n  public key  {}\n  created     {}\n",
                p.key_id, p.name, p.public_key, p.created_at
            );
            done(&p, s, EXIT_OK)
        }
        KeysCommand::Trust {
            file,
            add,
            valid_from,
            revoked_at,
        } => {
            let r = bellwether::trust_key(file, add, valid_from.as_deref(), revoked_at.as_deref())?;
            let window = match (&r.key.valid_from, &r.key.revoked_at) {
                (None, None) => "with no time limit".to_string(),
                (Some(f), None) => format!("for signatures made from {f}"),
                (None, Some(t)) => format!("for signatures made before {t}"),
                (Some(f), Some(t)) => format!("for signatures made from {f} until before {t}"),
            };
            let s = format!(
                "{} now trusts {} (key {}) {window}; it lists {} key(s)\n",
                r.file,
                r.key.name,
                short(&r.key.key_id),
                r.keys
            );
            done(&r, s, EXIT_OK)
        }
    }
}

/// The signing key `fix` uses, read from a path before anything runs; a
/// key that cannot be used is a usage error (exit 2).
pub(crate) fn signing_key(
    path: Option<&std::path::Path>,
) -> Result<Option<bellwether::SigningKey>, Failure> {
    path.map(bellwether::load_signing_key)
        .transpose()
        .map_err(|e: Error| Failure::from(e))
}

fn link(chain: &[ChainLink], kind: RecordKind) -> Option<&ChainLink> {
    chain.iter().find(|l| l.kind == kind)
}

/// `receipt show`: the chain, the signatures, and with
/// `--require-signature` exit 1 unless the receipt and its certificate are
/// each signed by a trusted key.
pub(crate) fn receipt_show(
    g: &Global,
    out: &Out,
    root: &str,
    receipt: &str,
    trust: &TrustArg,
    require_signature: bool,
) -> Result<Outcome, Failure> {
    let h = hash(receipt, "receipt")?;
    let trusted = trust.load()?;
    if require_signature && trusted.is_none() {
        return Err(Failure::Usage(
            "--require-signature checks the signatures against keys you trust; pass \
             --trusted-keys <file> (or set BELLWETHER_TRUSTED_KEYS), a file `bellwether keys \
             trust` writes"
                .into(),
        ));
    }
    let r = engine_keyed(g, root, None, trusted)?.receipt(&h)?;
    let mut text = human::receipt(&r, out);
    let mut code = EXIT_OK;
    if require_signature && r.signed != Some(true) {
        let why = require_refusal(&r);
        // --json and --quiet print no text; the refusal still says why.
        if out.json || out.quiet {
            eprintln!("bellwether: {why}");
        }
        let _ = writeln!(text, "refused: {why}");
        code = EXIT_FAILED;
    }
    done(&r, text, code)
}

/// Why `--require-signature` refused: each record's verdict, in words.
pub(crate) fn require_refusal(r: &ReceiptResult) -> String {
    let say = |kind: RecordKind| match link(&r.chain, kind).and_then(|l| l.verdict.as_ref()) {
        Some(v) => v.to_string(),
        None => "not readable, so not verified".to_string(),
    };
    format!(
        "--require-signature: the receipt and its certificate must each be signed by a trusted \
         key; receipt {}; certificate {}",
        say(RecordKind::Receipt),
        say(RecordKind::Certificate)
    )
}

/// The signature lines under a receipt's chain: the verdicts against the
/// trust file, or, without one, what is there and how to check it.
pub(crate) fn signature_lines(s: &mut String, r: &ReceiptResult) {
    let kinds = [RecordKind::Receipt, RecordKind::Certificate];
    if r.signed.is_some() {
        for k in kinds {
            let line = match link(&r.chain, k) {
                Some(l) => match &l.verdict {
                    Some(v) => v.to_string(),
                    None => "not readable, so not verified".to_string(),
                },
                None => "not in the chain".to_string(),
            };
            let _ = writeln!(s, "  {:<11}  {line}", k.as_str());
        }
        return;
    }
    let mut any = false;
    for k in kinds {
        let Some(sigs) = link(&r.chain, k).and_then(|l| l.signatures.as_ref()) else {
            continue;
        };
        if sigs.is_empty() {
            continue;
        }
        any = true;
        let valid = sigs
            .iter()
            .filter(|x| matches!(x, SignatureStatus::Valid { .. }))
            .count();
        let invalid = sigs.len() - valid;
        let mut line = format!("{valid} valid signature(s)");
        if invalid > 0 {
            let _ = write!(line, ", {invalid} INVALID");
        }
        let _ = writeln!(s, "  {:<11}  {line}", k.as_str());
    }
    if any {
        let _ = writeln!(
            s,
            "  pass --trusted-keys <file> to check who signed (`bellwether keys trust` writes it)"
        );
    } else {
        let _ = writeln!(
            s,
            "  unsigned: no signature beside the receipt or its certificate (`bellwether fix \
             --signing-key <file>` signs what it writes)"
        );
    }
}
