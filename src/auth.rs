use std::fs;

use crate::errors::VerError;

pub fn whoami() -> Result<(), VerError> {
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
    println!("{}", content);
    Ok(())
}
