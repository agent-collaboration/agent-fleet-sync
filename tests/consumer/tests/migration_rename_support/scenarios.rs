use super::*;
#[derive(Clone)]
pub struct RenameSchema {
    pub(super) migrations: &'static [Migration],
    pub(super) tables: &'static [Vec<TableName>],
    pub(super) count: Arc<AtomicUsize>,
    local: &'static [Migration],
}
impl ReplicatedSchema for RenameSchema {
    fn local_migrations(&self) -> &'static [Migration] {
        let version = self.count.load(Ordering::SeqCst) as i64;
        &self.local[..self
            .local
            .partition_point(|migration| migration.version <= version)]
    }
    fn migrations(&self) -> &'static [Migration] {
        self.migrations
            .get(..self.count.load(Ordering::SeqCst))
            .unwrap_or_default()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
            .get(self.count.load(Ordering::SeqCst).saturating_sub(1))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}
pub struct Step {
    pub(super) sql: &'static str,
    pub(super) tables: Vec<&'static str>,
    pub(super) write: &'static str,
}
pub struct Case {
    pub(super) steps: Vec<Step>,
    pub(super) cold: bool,
    pub(super) fresh: bool,
    pub(super) local_history: bool,
}
const BASE: &str = "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, body TEXT, amount INTEGER, payload BLOB, ratio REAL, missing TEXT, discarded TEXT)";
const BASE_WRITE: &str = "INSERT INTO records(id,body,amount,payload,ratio,missing,discarded) VALUES(?,?,?,X'00FF80',1.25,NULL,'discard me')";
fn step(sql: &'static str, write: &'static str) -> Step {
    Step {
        sql,
        tables: vec!["records"],
        write,
    }
}
fn case(sql: &'static str, write: &'static str) -> Case {
    Case {
        steps: vec![step(BASE, BASE_WRITE), step(sql, write)],
        cold: false,
        fresh: false,
        local_history: false,
    }
}
pub fn rename_table_case(cold: bool, fresh: bool) -> Case {
    let mut case = case(
        "ALTER TABLE records RENAME TO tasks",
        "UPDATE tasks SET body=?,amount=? WHERE id=?",
    );
    case.steps[1].tables = vec!["tasks"];
    case.cold = cold;
    case.fresh = fresh;
    if fresh {
        case.steps[0].sql = "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, body TEXT, amount INTEGER, payload BLOB, ratio REAL, missing TEXT, discarded TEXT); INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001','before',9007199254740993,X'00FF80',1.25,NULL,'discard me')";
    }
    case
}
pub fn rename_chain_case() -> Case {
    Case {
        steps: vec![
            Step {
                sql: "CREATE TABLE a(id TEXT NOT NULL PRIMARY KEY, body TEXT, amount INTEGER, payload BLOB, ratio REAL, missing TEXT, discarded TEXT); INSERT INTO a VALUES('019eef00-0000-7000-8000-000000000001','before',9007199254740993,X'00FF80',1.25,NULL,'discard me')",
                tables: vec!["a"],
                write: "UPDATE a SET body=?,amount=? WHERE id=?",
            },
            Step {
                sql: "ALTER TABLE a RENAME TO b",
                tables: vec!["b"],
                write: "UPDATE b SET body=?,amount=? WHERE id=?",
            },
            Step {
                sql: "ALTER TABLE b RENAME TO c",
                tables: vec!["c"],
                write: "UPDATE c SET body=?,amount=? WHERE id=?",
            },
        ],
        cold: false,
        fresh: true,
        local_history: false,
    }
}
pub(super) fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(3600),
        renew_interval: Duration::from_secs(30),
    }
}
pub(super) fn peer() -> TestResult<PeerId> {
    Ok(PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?)
}
pub(super) fn receiver(id: u128) -> TestResult<ReceiverId> {
    Ok(ReceiverId::new(durable_steps::Uuid::from_u128(
        (id) | 0x019eef00000070008000000000000000,
    ))?)
}
pub(super) fn schema(case: &Case) -> TestResult<RenameSchema> {
    let migrations = case
        .steps
        .iter()
        .enumerate()
        .map(|(i, s)| {
            Migration::new(
                i as i64 + 1,
                format!("normal {}", i + 1).into(),
                MigrationType::Simple,
                AssertSqlSafe(s.sql.to_owned()).into_sql_str(),
                false,
            )
        })
        .collect::<Vec<_>>();
    let tables = case
        .steps
        .iter()
        .map(|s| {
            s.tables
                .iter()
                .copied()
                .map(TableName::new)
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RenameSchema {
        migrations: Box::leak(migrations.into_boxed_slice()),
        local: if case.local_history {
            Box::leak(vec![
                Migration::new(1,"local create".into(),MigrationType::Simple,"CREATE TABLE private_history(id TEXT PRIMARY KEY,value TEXT); INSERT INTO private_history VALUES('local','private-local-only')".into_sql_str(),false),
                Migration::new(3,"local drop".into(),MigrationType::Simple,"DROP TABLE private_history".into_sql_str(),false),
            ].into_boxed_slice())
        } else {
            &[]
        },
        tables: Box::leak(tables.into_boxed_slice()),
        count: Arc::new(AtomicUsize::new(if case.fresh {
            case.steps.len()
        } else {
            1
        })),
    })
}
pub fn dropped_local_case() -> Case {
    let mut scenario = rename_table_case(false, false);
    scenario.local_history = true;
    scenario.steps.push(Step {
        sql: "CREATE INDEX tasks_body ON tasks(body)",
        tables: vec!["tasks"],
        write: "UPDATE tasks SET body=?,amount=? WHERE id=?",
    });
    scenario
}
