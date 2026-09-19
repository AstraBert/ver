use std::{env, fs};

#[cfg(feature = "minus")]
use minus::Pager;
#[cfg(feature = "minus")]
use std::fmt::Write;

use crate::commons::resolve_ver_dir_path;
use crate::errors::VerError;
use crate::init::SNAPSHOT_HISTORY_FILE;

#[cfg(feature = "minus")]
pub fn log(head: Option<usize>) -> Result<(), VerError> {
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let fl = resolved.join(SNAPSHOT_HISTORY_FILE);
    let content = fs::read_to_string(fl)?;
    let mut lines: Vec<&str> = content.lines().collect();
    lines.reverse();

    if let Some(h) = (head)
        && h <= lines.len()
    {
        let _ = lines.split_off(h);
    }

    let mut output = Pager::new();

    for l in lines {
        writeln!(output, "{}", l)?;
    }

    minus::page_all(output)?;

    Ok(())
}

#[cfg(not(feature = "minus"))]
pub fn log(head: Option<usize>) -> Result<(), VerError> {
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let fl = resolved.join(SNAPSHOT_HISTORY_FILE);
    let content = fs::read_to_string(fl)?;
    let mut lines: Vec<&str> = content.lines().collect();
    lines.reverse();

    if let Some(h) = (head)
        && h <= lines.len()
    {
        let _ = lines.split_off(h);
    }

    println!("{}", lines.join("\n"));

    Ok(())
}
