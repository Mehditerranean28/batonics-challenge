// tests/golden_smoke.rs
use std::process::Command;

#[test]
fn golden_smoke_runs_file_ndjson() {
    let tmp = tempfile::tempdir().unwrap();
    let in_path = tmp.path().join("tiny.ndjson");
    let out_path = tmp.path().join("out.json");

    std::fs::write(
        &in_path,
        r#"
{"type":"add","symbol":"TEST","side":"bid","oid":1,"px":100000,"qty":10,"seq":1,"ts_ns":1}
{"type":"add","symbol":"TEST","side":"ask","oid":2,"px":100100,"qty":5,"seq":2,"ts_ns":2}
{"type":"fill","symbol":"TEST","oid":1,"qty":3,"seq":3,"ts_ns":3}
{"type":"cancel","symbol":"TEST","oid":2,"qty":2,"seq":4,"ts_ns":4}
"#,
    )
    .unwrap();

    let exe = env!("CARGO_BIN_EXE_batonics-challenge");
    let status = Command::new(exe)
        .args([
            "run",
            "--file",
            in_path.to_str().unwrap(),
            "--http-bind",
            "127.0.0.1:0",
            "--out",
            out_path.to_str().unwrap(),
            "--depth",
            "0",
            "--snapshot-every-n",
            "1",
            "--snapshot-interval-ms",
            "0",
            "--shards",
            "2",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    let out = std::fs::read_to_string(&out_path).unwrap();
    assert!(out.contains(r#""type":"final""#));
    assert!(out.contains(r#""symbols""#));
    assert!(out.contains(r#""TEST""#));
}
