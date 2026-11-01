use std::io::Write;

use crate::{commons::read_history, context::read_context, errors::VerError};

pub fn log<O: Write>(
    tolerate_incomplete: bool,
    limit: Option<usize>,
    stdout: &mut O,
) -> Result<(), VerError> {
    let ctx = read_context()?;
    let opt = ctx.current_context();
    if opt.is_none() {
        return Err("No context is currently in use".into());
    }
    let (fs_des, _) = opt.unwrap();
    let fs = fs_des.build_fs()?;
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
