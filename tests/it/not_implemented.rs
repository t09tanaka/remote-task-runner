use assert_cmd::Command;

const CASES: &[&[&str]] = &[
    &["init"],
    &["ci"],
    &["ci", "plan"],
    &["ci", "--snapshot", "commit", "--ref", "HEAD"],
    &["exec", "--in-place", "--", "true"],
    &["logs", "x", "--follow"],
    &["wait", "x"],
    &["cancel", "x"],
    &["retry", "x"],
    &["outputs", "x", "--dir", "out"],
    &["estimate", "--", "true"],
    &["doctor"],
    &["workers"],
    &["workers", "add", "w", "--ssh", "u@h", "--install"],
    &["workers", "start", "w"],
    &["workers", "drain", "w"],
    &["workers", "resume", "w"],
    &["workers", "stop", "w", "--cancel-running"],
    &["workers", "upgrade", "w"],
    &["workers", "diff", "w"],
    &["workers", "sync", "w", "--project", "p"],
    &["workers", "forget", "w"],
    &["worker", "--stdio", "--ephemeral"],
];

#[test]
fn every_command_reports_not_implemented_as_json() {
    for args in CASES {
        let out = Command::cargo_bin("remote-task-runner")
            .unwrap()
            .arg("--json")
            .args(*args)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(70), "{args:?}");
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("stdout is JSON");
        assert_eq!(v["schema_version"], 1, "{args:?}");
        assert_eq!(v["ok"], false, "{args:?}");
        assert_eq!(v["error"]["code"], "NOT_IMPLEMENTED", "{args:?}");
        assert!(v["error"]["message"].is_string(), "{args:?}");
    }
}

#[test]
fn without_json_stdout_is_empty_and_error_goes_to_stderr() {
    let out = Command::cargo_bin("remote-task-runner")
        .unwrap()
        .arg("init")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(70));
    assert!(out.stdout.is_empty());
    assert!(!out.stderr.is_empty());
}

#[test]
fn bad_input_exits_with_input_error() {
    let out = Command::cargo_bin("remote-task-runner")
        .unwrap()
        .arg("exec")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}
