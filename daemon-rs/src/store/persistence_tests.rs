use super::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

const TIMEOUT: Duration = Duration::from_secs(10);

struct TestHome(PathBuf);

impl TestHome {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("agentsterm-save-test-{}", rand_hex(12))))
    }

    fn files(&self) -> StoreFiles {
        StoreFiles::new(self.0.clone())
    }
}

impl Drop for TestHome {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn create_tab(store: &StateStore) -> String {
    store
        .create_tab("shell", None, None, None, None, None, None)
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[test]
fn failed_message_and_partial_index_retry_without_another_mutation() {
    let home = TestHome::new();
    let fail_writes = Arc::new(AtomicBool::new(true));
    let failing = fail_writes.clone();
    let (saved, retried) = mpsc::channel();
    let store = StateStore::open(home.files(), move |path, bytes, mode| {
        let target = path.ends_with("messages.json") || path.ends_with("tabs.jsonl");
        if target && failing.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected write failure"));
        }
        write_atomic_file(path, bytes, mode)?;
        if target {
            saved.send(path.to_owned()).unwrap();
        }
        Ok(())
    });
    let id = create_tab(&store);
    let message = json!({"id":"one","text":"first"});
    store.add_message(&id, message.clone());
    assert!(store.save_now().is_err());
    assert!(home.files().workspace_path().is_file());
    assert!(!home.files().index_path().exists());
    fail_writes.store(false, Ordering::SeqCst);

    let paths: HashSet<_> = (0..2)
        .map(|_| retried.recv_timeout(TIMEOUT).unwrap())
        .collect();
    assert!(paths.contains(&home.files().messages_path(&id).unwrap()));
    assert!(paths.contains(&home.files().index_path()));
    let reopened = StateStore::open(home.files(), write_atomic_file);
    assert_eq!(reopened.get_messages(&id), [message]);
}

#[test]
fn mutation_during_snapshot_write_survives_following_flush() {
    let home = TestHome::new();
    let pause = Arc::new(AtomicBool::new(false));
    let pause_writer = pause.clone();
    let (started, writing) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let store = StateStore::open(home.files(), move |path, bytes, mode| {
        if path.ends_with("messages.json") && pause_writer.swap(false, Ordering::SeqCst) {
            started.send(()).unwrap();
            resume.recv_timeout(TIMEOUT).unwrap();
        }
        write_atomic_file(path, bytes, mode)
    });
    let id = create_tab(&store);
    store.save_now().unwrap();
    pause.store(true, Ordering::SeqCst);
    let first = json!({"id":"one","text":"first"});
    let second = json!({"id":"two","text":"second"});
    store.add_message(&id, first.clone());
    writing.recv_timeout(TIMEOUT).unwrap();
    store.add_message(&id, second.clone());
    release.send(()).unwrap();
    store.save_now().unwrap();
    let reopened = StateStore::open(home.files(), write_atomic_file);
    assert_eq!(reopened.get_messages(&id), [first, second]);
}

#[test]
fn resolve_abs_path_expands_tilde_and_pop_next_queued_is_fifo() {
    let home = home_dir().to_string_lossy().to_string();
    assert_eq!(resolve_abs_path("~"), resolve_abs_path(&home));
    assert_eq!(
        resolve_abs_path("~/project/sub/.."),
        resolve_abs_path(&format!("{home}/project"))
    );

    let test_home = TestHome::new();
    let store = StateStore::open(test_home.files(), write_atomic_file);
    let tab_id = create_tab(&store);
    assert!(store.pop_next_queued_message(&tab_id).is_none());

    store.enqueue_message(&tab_id, "first", None, &[]).unwrap();
    store.enqueue_message(&tab_id, "second", None, &[]).unwrap();
    store.enqueue_message(&tab_id, "third", None, &[]).unwrap();

    let (tab1, item1) = store.pop_next_queued_message(&tab_id).unwrap();
    assert_eq!(item1["text"].as_str(), Some("first"));
    assert_eq!(tab1["queuedMessages"].as_array().unwrap().len(), 2);
    assert_eq!(tab1["queuedMessages"][0]["text"].as_str(), Some("second"));

    let (tab2, item2) = store.pop_next_queued_message(&tab_id).unwrap();
    assert_eq!(item2["text"].as_str(), Some("second"));
    assert_eq!(tab2["queuedMessages"].as_array().unwrap().len(), 1);

    let (tab3, item3) = store.pop_next_queued_message(&tab_id).unwrap();
    assert_eq!(item3["text"].as_str(), Some("third"));
    assert!(tab3["queuedMessages"].as_array().unwrap().is_empty());
    assert!(store.pop_next_queued_message(&tab_id).is_none());
}

#[test]
fn model_throughput_survives_restart_without_tab_history() {
    let home = TestHome::new();
    let store = StateStore::open(home.files(), write_atomic_file);
    store.record_throughput("codex", "example", 120, 2000, |rates| {
        assert_eq!(rates[0]["tokensPerSecond"], 60.0);
    });
    store.save_now().unwrap();
    assert!(home.files().root.join("model-throughput.json").is_file());
    assert!(!home.files().workspace_path().exists());
    assert!(!home.files().index_path().exists());
    let reopened = StateStore::open(home.files(), write_atomic_file);
    reopened.with_throughput(|rates| {
        assert_eq!(rates.len(), 1);
        assert_eq!(rates[0]["model"], "example");
        assert_eq!(rates[0]["tokensPerSecond"], 60.0);
    });
}

#[test]
fn legacy_v1_throughput_file_is_discarded_and_rewritten_on_open() {
    let home = TestHome::new();
    let files = home.files();
    fs::create_dir_all(&files.root).unwrap();
    fs::write(
        files.throughput_path(),
        r#"{"claude":{"old-model":[{"at":1795830000000,"tokens":100,"duration_ms":2000000}]}}"#,
    )
    .unwrap();
    let store = StateStore::open(home.files(), write_atomic_file);
    store.with_throughput(|rates| {
        assert!(rates.is_empty());
    });
    store.save_now().unwrap();
    let rewritten: Value =
        serde_json::from_slice(&fs::read(files.throughput_path()).unwrap()).unwrap();
    assert_eq!(rewritten["version"], 2);
}
