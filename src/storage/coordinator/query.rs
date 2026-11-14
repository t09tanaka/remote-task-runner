//! Bounded metadata-only reads. No task specification or credential is returned.
use super::*;
use serde::{Deserialize, Serialize};

pub const MAX_QUERY_ROWS: u32 = 64;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub run_id: RunId,
    pub created_at_ms: u64,
    pub closed_at_ms: Option<u64>,
    pub task_count: u64,
    pub facts: RunFacts,
}
/// Whole-run facts from one read snapshot, independently of the displayed page.
/// A computed attempt can still have pending or failed collection. These facts
/// do not claim a final outcome; an empty run is never marked complete.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunFacts {
    pub computed_tasks: u64,
    pub collected_tasks: u64,
    pub uncertain_tasks: u64,
    pub non_successful_tasks: u64,
    pub collection_failed_tasks: u64,
    pub required_artifact_missing_tasks: u64,
    pub compute_complete: bool,
    pub collection_complete: bool,
}
fn run_facts(c: &Connection, run: &str, cancel: bool, collections: bool) -> Result<RunFacts> {
    // Same current-attempt selection and cancelled-unplaced exception as
    // run_standing. The engine aggregates without materializing task rows.
    let cancelled = if cancel {
        "t.cancel_requested_at_ms IS NOT NULL AND a.attempt_id IS NULL"
    } else {
        "0"
    };
    let (join, closed, verdict) = if collections {
        (
            "LEFT JOIN collection x ON x.attempt_id=a.attempt_id",
            "x.closed_at_ms IS NOT NULL AND x.verdict IS NOT NULL",
            "x.verdict",
        )
    } else {
        ("", "0", "NULL")
    };
    let sql=format!("SELECT count(*),coalesce(sum(a.termination_json IS NOT NULL OR ({cancelled})),0),coalesce(sum((a.termination_json IS NOT NULL AND ({closed}) AND {verdict}='complete') OR ({cancelled})),0),coalesce(sum(a.observation IN ('unknown','reconciling')),0),coalesce(sum((a.termination_json IS NOT NULL AND json_extract(a.termination_json,'$.outcome')<>'succeeded') OR ({cancelled})),0),coalesce(sum(a.termination_json IS NOT NULL AND ({closed}) AND {verdict}='gave_up'),0),coalesce(sum(a.termination_json IS NOT NULL AND ({closed}) AND {verdict}='required_missing'),0) FROM task t LEFT JOIN attempt a ON a.attempt_id=(SELECT aa.attempt_id FROM attempt aa WHERE aa.task_id=t.task_id AND aa.retired=0 ORDER BY aa.generation DESC LIMIT 1) {join} WHERE t.run_id=?1");
    let values = c.query_row(&sql, [run], |r| {
        Ok([
            r.get::<_, i64>(0)?,
            r.get(1)?,
            r.get(2)?,
            r.get(3)?,
            r.get(4)?,
            r.get(5)?,
            r.get(6)?,
        ])
    })?;
    let [total, computed, collected, uncertain, non_success, failed, missing]: [u64; 7] = values
        .map(to_u64)
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .try_into()
        .expect("seven aggregate columns");
    Ok(RunFacts {
        computed_tasks: computed,
        collected_tasks: collected,
        uncertain_tasks: uncertain,
        non_successful_tasks: non_success,
        collection_failed_tasks: failed,
        required_artifact_missing_tasks: missing,
        compute_complete: total > 0 && computed == total,
        collection_complete: total > 0 && collected == total,
    })
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub task_id: TaskId,
    pub run_id: RunId,
    pub created_at_ms: u64,
    pub cancel_requested_at_ms: Option<u64>,
    pub current_attempt: Option<CurrentAttempt>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentAttempt {
    pub attempt_id: AttemptId,
    pub state: AttemptState,
    pub observation: Observation,
    pub termination: Option<Termination>,
}
fn current(c: &Connection, task: &str) -> Result<Option<CurrentAttempt>> {
    let aid=c.query_row("SELECT attempt_id FROM attempt WHERE task_id=?1 AND retired=0 ORDER BY generation DESC LIMIT 1",[task],|r|r.get::<_,String>(0)).optional()?;
    aid.map(|aid| {
        let a = load_attempt(c, &aid)?.ok_or_else(|| not_found(format!("attempt {aid}")))?;
        Ok(CurrentAttempt {
            attempt_id: a.attempt_id,
            state: a.state,
            observation: a.observation,
            termination: a.termination,
        })
    })
    .transpose()
}
fn limit(n: u32) -> Result<i64> {
    if n == 0 || n > MAX_QUERY_ROWS {
        return Err(StorageError::Invalid("limit must be 1..64".into()));
    }
    Ok(i64::from(n) + 1)
}
fn page<T>(mut items: Vec<T>, n: u32, key: impl Fn(&T) -> String) -> Page<T> {
    let next_cursor = if items.len() > n as usize {
        items.truncate(n as usize);
        items.last().map(key)
    } else {
        None
    };
    Page { items, next_cursor }
}
impl CoordinatorStore {
    /// Read only bounded collection status, never result_json or logs_json.
    pub fn query_collection_metadata(
        &self,
        attempt: &AttemptId,
    ) -> Result<(Option<String>, Option<u64>)> {
        if !self.collections {
            return Ok((None, None));
        }
        let attempt = attempt.to_string();
        self.db.call(move |c| {
            let row=c.query_row("SELECT CASE verdict WHEN 'complete' THEN 'complete' WHEN 'required_missing' THEN 'required_missing' WHEN 'gave_up' THEN 'gave_up' WHEN NULL THEN NULL ELSE CASE WHEN verdict IS NULL THEN NULL ELSE 'invalid' END END,closed_at_ms FROM collection WHERE attempt_id=?1",[attempt],|r|Ok((r.get::<_,Option<String>>(0)?,r.get::<_,Option<i64>>(1)?))).optional()?;
            let Some((verdict,closed))=row else {return Ok((None,None));};
            if verdict.as_deref()==Some("invalid") {return Err(StorageError::Corrupt("invalid collection verdict".into()));}
            let closed=closed.map(to_u64).transpose()?;
            Ok((verdict.filter(|_|closed.is_some()),closed))
        })
    }

    pub fn query_runs(&self, n: u32, after: Option<RunId>) -> Result<Page<Run>> {
        let bound = limit(n)?;
        let (cancel, collections) = (self.task_cancel, self.collections);
        self.db.call(move |c| {
            let c=c.unchecked_transaction()?;
            let mut q=c.prepare("SELECT r.run_id,r.created_at_ms,r.closed_at_ms,(SELECT count(*) FROM task t WHERE t.run_id=r.run_id) FROM run r WHERE r.run_id > coalesce(?1,'') ORDER BY r.run_id LIMIT ?2")?;
            let raw=q.query_map(params![after.map(String::from),bound],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,Option<i64>>(2)?,r.get::<_,i64>(3)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
            let items=raw.into_iter().map(|(rid,created,closed,count)|Ok(Run{facts:run_facts(&c,&rid,cancel,collections)?,run_id:id("run",RunId::parse(&rid))?,created_at_ms:to_u64(created)?,closed_at_ms:closed.map(to_u64).transpose()?,task_count:to_u64(count)?})).collect::<Result<Vec<_>>>()?;
            Ok(page(items,n,|r|r.run_id.to_string()))
        })
    }
    pub fn query_run(&self, run: RunId) -> Result<Option<Run>> {
        let (cancel, collections) = (self.task_cancel, self.collections);
        self.db.call(move |c| {
            let c=c.unchecked_transaction()?;
            let raw=c.query_row("SELECT created_at_ms,closed_at_ms,(SELECT count(*) FROM task WHERE run_id=?1) FROM run WHERE run_id=?1",[run.as_str()],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,Option<i64>>(1)?,r.get::<_,i64>(2)?))).optional()?;
            raw.map(|(created,closed,count)|Ok(Run{facts:run_facts(&c,run.as_str(),cancel,collections)?,run_id:run,created_at_ms:to_u64(created)?,closed_at_ms:closed.map(to_u64).transpose()?,task_count:to_u64(count)?})).transpose()
        })
    }
    pub fn query_tasks(&self, run: RunId, n: u32, after: Option<TaskId>) -> Result<Page<Task>> {
        let bound = limit(n)?;
        let cancel = self.task_cancel;
        self.db.call(move |c| {
            if !c.query_row("SELECT EXISTS(SELECT 1 FROM run WHERE run_id=?1)",[run.as_str()],|r|r.get::<_,bool>(0))? { return Err(not_found(format!("run {run}"))); }
            let cancel=if cancel {"cancel_requested_at_ms"}else{"NULL"};
            let mut q=c.prepare(&format!("SELECT task_id,created_at_ms,{cancel} FROM task WHERE run_id=?1 AND task_id>coalesce(?2,'') ORDER BY task_id LIMIT ?3"))?;
            let raw=q.query_map(params![run.as_str(),after.map(String::from),bound],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,Option<i64>>(2)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
            let items=raw.into_iter().map(|(tid,created,cancel)|Ok(Task{current_attempt:current(c,&tid)?,task_id:id("task",TaskId::parse(&tid))?,run_id:run.clone(),created_at_ms:to_u64(created)?,cancel_requested_at_ms:cancel.map(to_u64).transpose()?})).collect::<Result<Vec<_>>>()?;
            Ok(page(items,n,|t|t.task_id.to_string()))
        })
    }
    pub fn query_task(&self, task: TaskId) -> Result<Option<Task>> {
        let cancel = self.task_cancel;
        self.db.call(move |c| {
            let cancel = if cancel {
                "cancel_requested_at_ms"
            } else {
                "NULL"
            };
            let raw = c
                .query_row(
                    &format!("SELECT run_id,created_at_ms,{cancel} FROM task WHERE task_id=?1"),
                    [task.as_str()],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, Option<i64>>(2)?,
                        ))
                    },
                )
                .optional()?;
            raw.map(|(run, created, cancel)| {
                Ok(Task {
                    current_attempt: current(c, task.as_str())?,
                    task_id: task,
                    run_id: id("run", RunId::parse(&run))?,
                    created_at_ms: to_u64(created)?,
                    cancel_requested_at_ms: cancel.map(to_u64).transpose()?,
                })
            })
            .transpose()
        })
    }
    /// Ordered by generation, including retired attempts. The cursor is a generation.
    pub fn query_attempts(
        &self,
        task: TaskId,
        n: u32,
        after: Option<Generation>,
    ) -> Result<Page<AttemptRecord>> {
        let bound = limit(n)?;
        self.db.call(move |c| {
            if !c.query_row("SELECT EXISTS(SELECT 1 FROM task WHERE task_id=?1)",[task.as_str()],|r|r.get::<_,bool>(0))? { return Err(not_found(format!("task {task}"))); }
            let mut q=c.prepare(&format!("SELECT {ATTEMPT_COLS} FROM attempt WHERE task_id=?1 AND (?2 IS NULL OR generation>?2) ORDER BY generation LIMIT ?3"))?;
            let mut rows=q.query(params![task.as_str(),after.map(|g|to_i64(g.0)).transpose()?,bound])?;
            let mut items=Vec::new(); while let Some(row)=rows.next()? { items.push(attempt_from_row(row)?); }
            Ok(page(items,n,|a|a.generation.0.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn metadata_pages_include_closed_runs_and_preserve_epoch() {
        let root = tempfile::tempdir().unwrap();
        let owner = CoordinatorStore::open(root.path(), 1).unwrap();
        let epoch = owner.epoch();
        let mut runs = Vec::new();
        for i in 0..3 {
            let run = RunId::generate();
            owner
                .create_run(&run, None, None, &json!({"secret":"DO_NOT_EXPOSE"}), i)
                .unwrap();
            owner.close_run(&run, i + 10).unwrap();
            runs.push(run);
        }
        runs.sort();
        let reader = CoordinatorStore::open_read_only(root.path()).unwrap();
        let first = reader.query_runs(2, None).unwrap();
        assert_eq!(first.items.len(), 2);
        assert_eq!(first.next_cursor.as_deref(), Some(runs[1].as_str()));
        assert!(first.items.iter().all(|r| r.closed_at_ms.is_some()));
        let last = reader.query_runs(2, Some(runs[1].clone())).unwrap();
        assert_eq!(last.items.len(), 1);
        assert!(last.next_cursor.is_none());
        assert_eq!(last.items[0].run_id, runs[2]);
        assert!(!serde_json::to_string(&first)
            .unwrap()
            .contains("DO_NOT_EXPOSE"));
        assert_eq!(reader.epoch(), epoch);
        assert_eq!(owner.epoch(), epoch);
        for n in [0, 65, u32::MAX] {
            assert!(reader.query_runs(n, None).is_err());
        }
        assert!(reader.query_run(RunId::generate()).unwrap().is_none());
    }
    #[test]
    fn task_pages_distinguish_missing_and_empty_parents() {
        let root = tempfile::tempdir().unwrap();
        let owner = CoordinatorStore::open(root.path(), 1).unwrap();
        let run = RunId::generate();
        owner.create_run(&run, None, None, &json!({}), 1).unwrap();
        assert!(owner
            .query_tasks(run.clone(), 1, None)
            .unwrap()
            .items
            .is_empty());
        assert!(matches!(
            owner.query_tasks(RunId::generate(), 1, None),
            Err(StorageError::NotFound { .. })
        ));
        let mut tasks = Vec::new();
        for _ in 0..3 {
            let t = TaskId::generate();
            owner
                .create_task(&t, &run, &json!({"argv":["PRIVATE"]}), 2)
                .unwrap();
            tasks.push(t);
        }
        tasks.sort();
        let first = owner.query_tasks(run.clone(), 2, None).unwrap();
        assert_eq!(first.next_cursor.as_deref(), Some(tasks[1].as_str()));
        let last = owner.query_tasks(run, 2, Some(tasks[1].clone())).unwrap();
        assert_eq!(last.items[0].task_id, tasks[2]);
        assert!(last.next_cursor.is_none());
        assert_eq!(
            owner.query_task(tasks[0].clone()).unwrap().unwrap().task_id,
            tasks[0]
        );
        assert!(owner.query_task(TaskId::generate()).unwrap().is_none());
        let empty = owner.query_attempts(tasks[0].clone(), 1, None).unwrap();
        assert!(empty.items.is_empty());
        assert!(empty.next_cursor.is_none());
        assert!(matches!(
            owner.query_attempts(TaskId::generate(), 1, None),
            Err(StorageError::NotFound { .. })
        ));
        assert!(!serde_json::to_string(&first).unwrap().contains("PRIVATE"));
    }
    #[test]
    fn history_keeps_retired_generations_and_resumes_exclusively() {
        let root = tempfile::tempdir().unwrap();
        let owner = CoordinatorStore::open(root.path(), 1).unwrap();
        let run = RunId::generate();
        let task = TaskId::generate();
        owner.create_run(&run, None, None, &json!({}), 1).unwrap();
        owner.create_task(&task, &run, &json!({}), 1).unwrap();
        for at in 2..5 {
            let aid = AttemptId::generate();
            owner
                .create_attempt(
                    NewAttempt {
                        attempt_id: aid.clone(),
                        task_id: task.clone(),
                        worker_id: WorkerId::parse("test").unwrap(),
                        payload_hash: Digest::of_bytes(b"payload"),
                        permit_id: None,
                    },
                    at,
                )
                .unwrap();
            owner.begin_retire(&aid, "test", at).unwrap();
            owner.confirm_retire(&aid, at).unwrap();
        }
        let first = owner.query_attempts(task.clone(), 2, None).unwrap();
        assert_eq!(first.items.len(), 2);
        assert!(first.items.iter().all(|a| a.retired));
        assert_eq!(first.next_cursor.as_deref(), Some("2"));
        let next = owner.query_attempts(task, 2, Some(Generation(2))).unwrap();
        assert_eq!(next.items.len(), 1);
        assert_eq!(next.items[0].generation, Generation(3));
        assert!(next.next_cursor.is_none());
    }
    #[test]
    fn whole_run_facts_include_failures_uncertainty_and_uncollected_outside_page() {
        use crate::results::plan::Verdict;
        use crate::state::Outcome;
        let root = tempfile::tempdir().unwrap();
        let owner = CoordinatorStore::open(root.path(), 1).unwrap();
        let run = RunId::generate();
        owner.create_run(&run, None, None, &json!({}), 1).unwrap();
        let mut tasks = (0..3).map(|_| TaskId::generate()).collect::<Vec<_>>();
        tasks.sort();
        let mut aids = Vec::new();
        for (index, task) in tasks.iter().enumerate() {
            owner.create_task(task, &run, &json!({}), 1).unwrap();
            let aid = AttemptId::generate();
            aids.push(aid.clone());
            owner
                .create_attempt(
                    NewAttempt {
                        attempt_id: aid.clone(),
                        task_id: task.clone(),
                        worker_id: WorkerId::parse("test").unwrap(),
                        payload_hash: Digest::of_bytes(b"payload"),
                        permit_id: None,
                    },
                    2,
                )
                .unwrap();
            if index == 2 {
                owner.set_observation(&aid, Observation::Unknown).unwrap();
                continue;
            }
            for state in [
                AttemptState::Preparing,
                AttemptState::Ready,
                AttemptState::Starting,
                AttemptState::Active,
                AttemptState::Finalizing,
            ] {
                owner.transition_attempt(&aid, state, 3).unwrap();
            }
            let outcome = if index == 0 {
                Outcome::Succeeded
            } else {
                Outcome::Failed
            };
            owner
                .finalize_attempt(&aid, Termination::new(outcome, None, None).unwrap(), 4)
                .unwrap();
            if index == 0 {
                let digest = Digest::of_bytes(b"result");
                owner
                    .begin_collection(&aid, &digest, &json!({}), &[], &[], 5)
                    .unwrap();
                owner
                    .close_collection(&aid, Some(&digest), Verdict::Complete, true, 6)
                    .unwrap();
            }
        }
        let first = owner.query_tasks(run.clone(), 1, None).unwrap();
        assert_eq!(first.items[0].task_id, tasks[0]);
        assert_eq!(
            first.items[0]
                .current_attempt
                .as_ref()
                .unwrap()
                .termination
                .as_ref()
                .unwrap()
                .outcome(),
            Outcome::Succeeded
        );
        assert!(first.next_cursor.is_some());
        let summary = owner.query_run(run.clone()).unwrap().unwrap();
        let facts = summary.facts;
        assert_eq!(summary.task_count, 3);
        assert_eq!(facts.computed_tasks, 2);
        assert_eq!(facts.collected_tasks, 1);
        assert_eq!(facts.uncertain_tasks, 1);
        assert_eq!(facts.non_successful_tasks, 1);
        assert!(!facts.compute_complete);
        assert!(!facts.collection_complete);
        let listed = owner.query_runs(1, None).unwrap();
        assert!(!listed.items[0].facts.collection_complete);
        owner
            .close_collection(&aids[1], None, Verdict::GaveUp, false, 7)
            .unwrap();
        for state in [
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ] {
            owner.transition_attempt(&aids[2], state, 8).unwrap();
        }
        owner
            .finalize_attempt(
                &aids[2],
                Termination::new(Outcome::Succeeded, None, None).unwrap(),
                9,
            )
            .unwrap();
        let digest = Digest::of_bytes(b"missing-result");
        owner
            .begin_collection(&aids[2], &digest, &json!({}), &[], &[], 10)
            .unwrap();
        owner
            .close_collection(&aids[2], Some(&digest), Verdict::RequiredMissing, true, 11)
            .unwrap();
        let facts = owner.query_run(run).unwrap().unwrap().facts;
        assert!(facts.compute_complete);
        assert!(!facts.collection_complete);
        assert_eq!(facts.collected_tasks, 1);
        assert_eq!(facts.collection_failed_tasks, 1);
        assert_eq!(facts.required_artifact_missing_tasks, 1);
        let aid = aids[0].to_string();
        owner.db.write(move |tx|{tx.execute("UPDATE collection SET result_json=zeroblob(8388608),logs_json=zeroblob(8388608) WHERE attempt_id=?1",[aid])?;Ok(())}).unwrap();
        let response = owner
            .query(crate::service::query::Query::Attempts {
                task_id: tasks[0].clone(),
                limit: 64,
                after: None,
            })
            .unwrap();
        let bytes = serde_json::to_vec(&response).unwrap();
        assert!(bytes.len() < crate::service::resident::MAX_REQUEST_BYTES);
        let crate::service::query::Response::Attempts { page } = response else {
            panic!("wrong response")
        };
        assert_eq!(
            page.items[0].collection_verdict.as_deref(),
            Some("complete")
        );
        assert_eq!(page.items[0].collection_closed_at_ms, Some(6));
    }
}
