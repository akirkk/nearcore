//! Bulk-dump every wasm contract reachable from flat storage on the local node.
//!
//! Pass 1 range-scans flat storage at the `CONTRACT_CODE` and `GLOBAL_CONTRACT_CODE`
//! prefixes per shard. The flat-state row carries either the full bytes (small,
//! inlined values) or a `ValueRef` whose `hash`/`length` are enough to dedupe
//! without touching the wasm. Pass 2 fetches each unique blob from `DBCol::State`.
//!
//! Designed to run in `Mode::ReadOnly` against a live RPC node's data dir;
//! all writes are local files in `--output`.

use crate::util::load_trie;
use anyhow::{Context, anyhow};
use borsh::BorshDeserialize;
use indicatif::{ProgressBar, ProgressStyle};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::GlobalContractCodeIdentifier;
use near_primitives::trie_key::col;
use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_contract_code_key;
use near_primitives::types::AccountId;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use nearcore::NearConfig;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(clap::Parser)]
pub struct DumpContractsCmd {
    /// Output directory. Created if missing. Wasm blobs land in `<output>/wasm/<hash>.wasm`.
    /// `<output>/accounts.jsonl` lists per-account contract refs (streamed during scan).
    /// `<output>/globals.jsonl` lists per-global-contract refs (streamed during scan).
    /// `<output>/summary.json` is written at the end.
    #[clap(long, value_parser)]
    output: PathBuf,
    /// Restrict scan to these ShardUIds (e.g. `s0.v3`). Default: all shards in the head's layout.
    #[clap(long)]
    shards: Option<Vec<ShardUId>>,
    /// Skip pass 2: don't fetch wasm blobs, only emit the manifests.
    #[clap(long)]
    no_blobs: bool,
}

impl DumpContractsCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        if let Err(err) = self.run_inner(home_dir, near_config, store) {
            eprintln!("dump-contracts failed: {err:#}");
            std::process::exit(1);
        }
    }

    fn run_inner(
        self,
        home_dir: &Path,
        near_config: NearConfig,
        store: Store,
    ) -> anyhow::Result<()> {
        let (epoch_manager, _runtime, _state_roots, header) =
            load_trie(store.clone(), home_dir, &near_config);
        let shard_layout = epoch_manager.get_shard_layout(header.epoch_id())?;
        let mut shard_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();
        if let Some(filter) = &self.shards {
            shard_uids.retain(|uid| filter.contains(uid));
            if shard_uids.is_empty() {
                return Err(anyhow!("--shards filter excluded every known shard"));
            }
        }

        fs::create_dir_all(&self.output)
            .with_context(|| format!("creating {}", self.output.display()))?;
        let blobs_dir = self.output.join("wasm");
        fs::create_dir_all(&blobs_dir)
            .with_context(|| format!("creating {}", blobs_dir.display()))?;
        let accounts_path = self.output.join("accounts.jsonl");
        let globals_path = self.output.join("globals.jsonl");
        let mut accounts_out = BufWriter::new(File::create(&accounts_path)?);
        let mut globals_out = BufWriter::new(File::create(&globals_path)?);

        eprintln!(
            "scanning {} shard(s) at block {} (height {}); writing to {}",
            shard_uids.len(),
            header.hash(),
            header.height(),
            self.output.display(),
        );

        let flat = store.flat_store();
        let trie_store = store.trie_store();

        // hash -> (shard_uid where we saw it, length in bytes). For Inlined values
        // we already wrote the blob; for Ref values we still need pass 2.
        let mut written: HashMap<CryptoHash, u64> = HashMap::new();
        let mut pending: HashMap<CryptoHash, ShardUId> = HashMap::new();

        let scan_pb = make_spinner();
        scan_pb.set_prefix("scan");

        let mut account_refs = 0u64;
        for shard_uid in &shard_uids {
            scan_pb.set_message(format!("shard {shard_uid} CONTRACT_CODE"));
            let from = [col::CONTRACT_CODE];
            let to = [col::CONTRACT_CODE + 1];
            for (raw_key, value) in flat.iter_range(*shard_uid, Some(&from), Some(&to)) {
                let account_id = parse_account_id_from_contract_code_key(&raw_key)
                    .with_context(|| format!("parsing CONTRACT_CODE key: {raw_key:?}"))?;
                let (hash, length) = absorb_value(
                    &value,
                    *shard_uid,
                    &blobs_dir,
                    self.no_blobs,
                    &mut written,
                    &mut pending,
                )?;
                serde_json::to_writer(
                    &mut accounts_out,
                    &AccountEntry {
                        account_id: &account_id,
                        code_hash: hash,
                        code_length: length,
                        shard_uid: *shard_uid,
                    },
                )?;
                writeln!(accounts_out)?;
                account_refs += 1;
                scan_pb.set_position(account_refs);
            }
        }
        accounts_out.flush()?;

        let mut global_refs = 0u64;
        for shard_uid in &shard_uids {
            scan_pb.set_message(format!("shard {shard_uid} GLOBAL_CONTRACT_CODE"));
            let from = [col::GLOBAL_CONTRACT_CODE];
            let to = [col::GLOBAL_CONTRACT_CODE + 1];
            for (raw_key, value) in flat.iter_range(*shard_uid, Some(&from), Some(&to)) {
                let identifier = GlobalContractCodeIdentifier::try_from_slice(&raw_key[1..])
                    .with_context(|| {
                        format!("decoding GLOBAL_CONTRACT_CODE identifier: {raw_key:?}")
                    })?;
                let (hash, length) = absorb_value(
                    &value,
                    *shard_uid,
                    &blobs_dir,
                    self.no_blobs,
                    &mut written,
                    &mut pending,
                )?;
                serde_json::to_writer(
                    &mut globals_out,
                    &GlobalEntry {
                        identifier: GlobalIdRepr::from(&identifier),
                        code_hash: hash,
                        code_length: length,
                        shard_uid: *shard_uid,
                    },
                )?;
                writeln!(globals_out)?;
                global_refs += 1;
                scan_pb.set_position(account_refs + global_refs);
            }
        }
        globals_out.flush()?;
        scan_pb.finish_with_message(format!(
            "{} account refs, {} global refs, {} unique inlined, {} unique to fetch",
            account_refs,
            global_refs,
            written.len(),
            pending.len(),
        ));

        if !self.no_blobs && !pending.is_empty() {
            let fetch_pb = ProgressBar::new(pending.len() as u64);
            fetch_pb.set_style(
                ProgressStyle::with_template(
                    "{prefix:>10} [{bar:40.cyan/blue}] {pos}/{len} · {per_sec} · {elapsed} · ETA {eta} · {msg}",
                )
                .unwrap()
                .progress_chars("=> "),
            );
            fetch_pb.set_prefix("fetch wasm");
            fetch_pb.enable_steady_tick(Duration::from_millis(200));
            for (hash, shard_uid) in &pending {
                let bytes = trie_store
                    .get(*shard_uid, hash)
                    .with_context(|| format!("fetching {hash} from shard {shard_uid}"))?;
                let path = blobs_dir.join(format!("{hash}.wasm"));
                fs::write(&path, bytes.as_ref())
                    .with_context(|| format!("writing {}", path.display()))?;
                written.insert(*hash, bytes.len() as u64);
                fetch_pb.inc(1);
            }
            fetch_pb.finish_with_message("done");
        }

        let summary = Summary {
            block_hash: *header.hash(),
            block_height: header.height(),
            shard_uids: shard_uids.iter().map(ShardUId::to_string).collect(),
            account_contract_refs: account_refs,
            global_contract_refs: global_refs,
            unique_blobs: written.len() as u64,
            blobs_written: !self.no_blobs,
        };
        let summary_path = self.output.join("summary.json");
        let summary_file = File::create(&summary_path)?;
        serde_json::to_writer_pretty(BufWriter::new(summary_file), &summary)?;

        eprintln!(
            "done · {} account refs · {} global refs · {} unique wasm blobs · summary at {}",
            account_refs,
            global_refs,
            written.len(),
            summary_path.display(),
        );
        Ok(())
    }
}

fn make_spinner() -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template(
            "{prefix:>10} {spinner} {pos} entries · {per_sec} · {elapsed} · {msg}",
        )
        .unwrap(),
    );
    pb.enable_steady_tick(Duration::from_millis(200));
    pb
}

fn absorb_value(
    value: &FlatStateValue,
    shard_uid: ShardUId,
    blobs_dir: &Path,
    no_blobs: bool,
    written: &mut HashMap<CryptoHash, u64>,
    pending: &mut HashMap<CryptoHash, ShardUId>,
) -> anyhow::Result<(CryptoHash, u64)> {
    match value {
        FlatStateValue::Inlined(bytes) => {
            let hash = CryptoHash::hash_bytes(bytes);
            let length = bytes.len() as u64;
            if !no_blobs && !written.contains_key(&hash) {
                let path = blobs_dir.join(format!("{hash}.wasm"));
                fs::write(&path, bytes.as_slice())
                    .with_context(|| format!("writing {}", path.display()))?;
                written.insert(hash, length);
                pending.remove(&hash);
            }
            Ok((hash, length))
        }
        FlatStateValue::Ref(value_ref) => {
            let hash = value_ref.hash;
            let length = value_ref.length as u64;
            if !no_blobs && !written.contains_key(&hash) {
                pending.entry(hash).or_insert(shard_uid);
            }
            Ok((hash, length))
        }
    }
}

#[derive(Serialize)]
struct AccountEntry<'a> {
    account_id: &'a AccountId,
    code_hash: CryptoHash,
    code_length: u64,
    shard_uid: ShardUId,
}

#[derive(Serialize)]
struct GlobalEntry<'a> {
    identifier: GlobalIdRepr<'a>,
    code_hash: CryptoHash,
    code_length: u64,
    shard_uid: ShardUId,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "value")]
enum GlobalIdRepr<'a> {
    CodeHash(CryptoHash),
    AccountId(&'a AccountId),
}

impl<'a> From<&'a GlobalContractCodeIdentifier> for GlobalIdRepr<'a> {
    fn from(identifier: &'a GlobalContractCodeIdentifier) -> Self {
        match identifier {
            GlobalContractCodeIdentifier::CodeHash(hash) => GlobalIdRepr::CodeHash(*hash),
            GlobalContractCodeIdentifier::AccountId(account_id) => {
                GlobalIdRepr::AccountId(account_id)
            }
        }
    }
}

#[derive(Serialize)]
struct Summary {
    block_hash: CryptoHash,
    block_height: u64,
    shard_uids: Vec<String>,
    account_contract_refs: u64,
    global_contract_refs: u64,
    unique_blobs: u64,
    blobs_written: bool,
}
