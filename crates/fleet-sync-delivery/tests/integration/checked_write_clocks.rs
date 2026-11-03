use std::{
    fs,
    path::{Path, PathBuf},
};

fn sql_files(directory: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            paths.extend(sql_files(&path));
        } else if path.extension().is_some_and(|extension| extension == "sql") {
            paths.push(path);
        }
    }
    paths
}

#[test]
fn checked_write_queries_never_evaluate_the_sql_wall_clock() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    for (package, folder) in [
        ("fleet-sync-delivery", "sql"),
        ("fleet-sync-lease", "sql"),
        ("fleet-sync-session", "sql"),
        ("durable-steps", "queries"),
    ] {
        let mut writes = 0;
        for path in sql_files(&workspace.join(package).join(folder)) {
            let sql = fs::read_to_string(&path).unwrap();
            let statement = sql
                .lines()
                .filter(|line| !line.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join(" ")
                .trim_start()
                .to_ascii_lowercase();
            if ["insert", "update", "delete", "replace"]
                .iter()
                .any(|prefix| statement.starts_with(prefix))
            {
                writes += 1;
                assert!(
                    !(statement.contains("strftime") && statement.contains("'now'")),
                    "{}: write timestamps must be bound from the owning injected clock",
                    path.display()
                );
            }
        }
        assert!(
            writes > 0,
            "{package}: write-query discovery returned no subjects"
        );
    }
}
