use std::io::Write;

use crate::{
    commons::{current_fs_kv, read_history},
    errors::VerError,
};

pub fn log<O: Write>(
    tolerate_incomplete: bool,
    limit: Option<usize>,
    stdout: &mut O,
) -> Result<(), VerError> {
    let (fs, _) = current_fs_kv()?;
    let (mut logs, is_incomplete) = read_history(tolerate_incomplete, &fs)?;
    if let Some(l) = limit {
        logs.truncate(l);
    }
    for l in logs {
        writeln!(stdout, "{}", l)?;
    }
    if is_incomplete {
        writeln!(
            stdout,
            "WARNING: some logs are missing from the reported history. If you want this to be an error, do not pass --incomplete-ok as a flag"
        )?;
    }

    Ok(())
}
