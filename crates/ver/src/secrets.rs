use std::{
    collections::HashMap,
    env, fs,
    io::{Read, Write},
    path::PathBuf,
    str::FromStr,
    sync::OnceLock,
};

use age::secrecy::ExposeSecret;
use redb::Value;

use crate::{commons::resolve_ver_dir_path, errors::VerError};

const VER_LOCAL_SECRETS: &str = ".versecrets.json";
static VER_RECIPIENTS_FILE: OnceLock<PathBuf> = OnceLock::new();
static VER_KEY_FILE: OnceLock<PathBuf> = OnceLock::new();

fn ver_recipients_file() -> &'static PathBuf {
    VER_RECIPIENTS_FILE.get_or_init(|| {
        dirs::home_dir()
            .expect("home directory should be resolved")
            .join(".ver")
            .join("recipients.txt")
    })
}

fn ver_key_file() -> &'static PathBuf {
    VER_KEY_FILE.get_or_init(|| {
        dirs::home_dir()
            .expect("home directory should be resolved")
            .join(".ver")
            .join("keys.txt")
    })
}

fn create_global_ver_dir_if_not_exists() -> Result<(), VerError> {
    let parent = ver_recipients_file().parent().unwrap();
    if !parent.exists() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

pub fn init_secrets<O: Write>(
    key_file: Option<String>,
    key_name: String,
    std_out: &mut O,
) -> Result<(), VerError> {
    create_global_ver_dir_if_not_exists()?;
    if let Some(key) = key_file {
        let content = fs::read_to_string(&key)?;
        let lines = content
            .lines()
            .filter(|l| !l.starts_with("# "))
            .collect::<Vec<&str>>();
        let key = lines.first().expect("Private key should be present");
        let identity = age::x25519::Identity::from_str(*key)?;
        fs::write(
            ver_recipients_file(),
            format!("# {}\n{}", key_name, identity.to_public().to_string()),
        )?;
        writeln!(std_out, "Secrets have been successfully initialized")?;
        return Ok(());
    }
    let new_key = age::x25519::Identity::generate();
    let private = new_key.to_string();
    let public = new_key.to_public().to_string();
    let content = format!(
        "# created: {}\n# public key: {}\n{}",
        chrono::Utc::now().format("%d/%m/%Y %H:%M:%S"),
        public,
        private.expose_secret()
    );
    fs::write(ver_key_file(), content)?;
    fs::write(
        ver_recipients_file(),
        format!("# {}\n{}\n", key_name, public),
    )?;
    writeln!(std_out, "Secrets have been successfully initialized")?;

    Ok(())
}

fn get_recipients() -> Result<Vec<age::x25519::Recipient>, VerError> {
    let rec_file = ver_recipients_file();

    if !rec_file.exists() {
        return Err(
            "There is no recipients file as of now: please run `ver secrets init` to add it".into(),
        );
    }

    let content = fs::read_to_string(rec_file)?;
    let lines: Vec<age::x25519::Recipient> = content
        .lines()
        .filter(|l| !l.starts_with("# "))
        .map(|s| age::x25519::Recipient::from_str(s.trim()))
        .collect::<Result<_, _>>()?;

    Ok(lines)
}

fn get_private_key(key_file: Option<String>) -> Result<String, VerError> {
    if let Some(key) = key_file {
        let content = fs::read_to_string(&key)?;
        let lines = content
            .lines()
            .filter(|l| !l.starts_with("# "))
            .collect::<Vec<&str>>();
        let key = lines.first().expect("Private key should be present");
        let identity = age::x25519::Identity::from_str(*key)?;
        return Ok(identity.to_string().expose_secret().to_owned());
    }
    let key_file = ver_key_file();

    if !key_file.exists() {
        return Err("No key file provided and global key file under $HOME/.ver/keys.txt does not exist: please run `ver secrets init` and try again".into());
    }

    let content = fs::read_to_string(ver_key_file())?;
    let lines = content
        .lines()
        .filter(|l| !l.starts_with("# "))
        .collect::<Vec<&str>>();
    let key = lines.first().expect("Private key should be present");
    let identity = age::x25519::Identity::from_str(*key)?;

    Ok(identity.to_string().expose_secret().to_owned())
}

fn decrypt(secret_key: &str, armored: &str) -> Result<Vec<u8>, VerError> {
    let identity = age::x25519::Identity::from_str(secret_key)?;

    let decryptor = age::Decryptor::new(age::armor::ArmoredReader::new(armored.as_bytes()))?;
    let mut reader = decryptor.decrypt(std::iter::once(&identity as &dyn age::Identity))?;

    let mut plaintext = Vec::new();
    reader.read_to_end(&mut plaintext)?;
    Ok(plaintext)
}

fn encrypt(recipients: &[age::x25519::Recipient], plaintext: &[u8]) -> Result<String, VerError> {
    let encryptor =
        age::Encryptor::with_recipients(recipients.iter().map(|r| r as &dyn age::Recipient))?;

    let mut out = Vec::new();
    let armored = age::armor::ArmoredWriter::wrap_output(&mut out, age::armor::Format::AsciiArmor)?;
    let mut writer = encryptor.wrap_output(armored)?;
    writer.write_all(plaintext)?;
    writer.finish().and_then(|armored| armored.finish())?;

    Ok(String::from_utf8(out)?)
}

pub fn add_recipient<O: Write>(
    name: String,
    key: String,
    key_file: Option<String>,
    std_out: &mut O,
) -> Result<(), VerError> {
    create_global_ver_dir_if_not_exists()?;
    let mut fl = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(ver_recipients_file())?;

    writeln!(fl, "# {}\n{}\n", name, key)?;

    let local_secrets = resolve_ver_dir_path(env::current_dir()?)?
        .parent()
        .unwrap()
        .join(VER_LOCAL_SECRETS);

    if !local_secrets.exists() {
        writeln!(
            std_out,
            "Added new recipient\nNo secrets to re-encrypt, exiting..."
        )?;
        return Ok(());
    }

    let secrets: HashMap<String, String> =
        serde_json::from_str(&fs::read_to_string(&local_secrets)?)?;

    if secrets.is_empty() {
        writeln!(
            std_out,
            "Added new recipient\nNo secrets to re-encrypt, exitig..."
        )?;
        return Ok(());
    }

    let recipients = get_recipients()?;
    let private_key = get_private_key(key_file)?;

    let mut new_secrets: HashMap<String, String> = HashMap::new();

    for (k, v) in secrets {
        let decrypted = decrypt(&private_key, &v)?;
        let secret = String::from_bytes(decrypted.as_slice());
        if !secret.starts_with(&format!("{}/", k)) {
            return Err(format!("Secret for key {} does not belong to it", k).into());
        }
        let re_encrypted = encrypt(&recipients, &decrypted)?;
        new_secrets.insert(k, re_encrypted);
    }

    fs::write(&local_secrets, serde_json::to_string_pretty(&new_secrets)?)?;

    writeln!(
        std_out,
        "Added new recipient and re-encrypted secrets for the new public key."
    )?;

    Ok(())
}

pub fn set_secret<O: Write>(
    secret_name: &str,
    secret_value: &str,
    std_out: &mut O,
) -> Result<(), VerError> {
    let local_secrets = resolve_ver_dir_path(env::current_dir()?)?
        .parent()
        .unwrap()
        .join(VER_LOCAL_SECRETS);

    let mut current_secrets = if local_secrets.exists() {
        serde_json::from_str::<HashMap<String, String>>(&fs::read_to_string(&local_secrets)?)?
    } else {
        HashMap::new()
    };

    let recipients = get_recipients()?;
    let encrypted = encrypt(
        &recipients,
        format!("{}/{}", secret_name, secret_value).as_bytes(),
    )?;

    current_secrets.insert(secret_name.to_owned(), encrypted);

    fs::write(
        local_secrets,
        serde_json::to_string_pretty(&current_secrets)?,
    )?;

    writeln!(std_out, "Added local secret for {}", secret_name)?;

    Ok(())
}

pub fn get_secret<O: Write>(
    name: &str,
    key_file: Option<String>,
    print: bool,
    std_out: &mut O,
) -> Result<String, VerError> {
    let local_secrets = resolve_ver_dir_path(env::current_dir()?)?
        .parent()
        .unwrap()
        .join(VER_LOCAL_SECRETS);

    if !local_secrets.exists() {
        return Err(
            "No secrets in the current repository: run `ver secrets set` to add one".into(),
        );
    }

    let current_secrets: HashMap<String, String> =
        serde_json::from_str(&fs::read_to_string(&local_secrets)?)?;

    if let Some(secret) = current_secrets.get(name) {
        let private_key = get_private_key(key_file)?;
        let decrypted = decrypt(&private_key, secret)?;
        let s = String::from_bytes(&decrypted);
        let prefix = format!("{}/", name);
        if !s.starts_with(&prefix) {
            return Err(
                format!("The secret associated with {} does not belong to it", name).into(),
            );
        }
        let secret = s.trim_start_matches(&prefix);

        if print {
            writeln!(std_out, "{}", secret)?;
        }

        return Ok(secret.to_owned());
    }

    Err(format!("Could not find secret associated with {}: run `ver secrets set '{}' <SECRET-VALUE>` to add it", name, name).into())
}
