use std::{collections::HashSet, fs::File};

use anyhow::Result;
use dbn::decode::dbn::Decoder;
use dbn::decode::DecodeRecordRef;
use dbn::{RecordRefEnum, VersionUpgradePolicy};

fn main() -> Result<()> {
    let f = File::open("data/feed.bin")?;
    let mut dec = Decoder::with_upgrade_policy(f, VersionUpgradePolicy::UpgradeToV3)?;

    let mut insts = HashSet::<u32>::new();
    let mut mbo = 0u64;
    let mut mbp10 = 0u64;

    while let Some(rec) = dec.decode_record_ref()? {
        let Ok(e) = rec.as_enum() else { continue };
        match e {
            RecordRefEnum::Mbo(m) => {
                insts.insert(m.hd.instrument_id);
                mbo += 1;
            }
            RecordRefEnum::Mbp10(m) => {
                insts.insert(m.hd.instrument_id);
                mbp10 += 1;
            }
            _ => {}
        }
    }

    println!("unique_instruments={}", insts.len());
    println!("mbo={}", mbo);
    println!("mbp10={}", mbp10);
    Ok(())
}
