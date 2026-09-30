use std::{fs, io};

use crate::errors::VerError;

pub fn whoami<O: io::Write>(std_out: &mut O) -> Result<(), VerError> {
    let user_file = dirs::home_dir()
        .expect("Should be able to resolve home directory")
        .join("ver")
        .join("userprofile");
    if !user_file.exists() {
        return Err(
            "You are not currently authenticated. Please run `ver login` and try again.".into(),
        );
    }
    let content = fs::read_to_string(user_file)?;
    writeln!(std_out, "{}", content)?;
    Ok(())
}
