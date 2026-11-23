use std::{fs, path::PathBuf};

use ed25519_dalek::{Signer, SigningKey};
use rcaster_app::config::validate_private_key_permissions;
use rcaster_service::upgrade::{SignedUpgradeManifest, UpgradeManifest};
use serde_json::Value;

fn main() {
    if let Err(error) = run() {
        eprintln!("rcaster-upgrade-signer: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let private_key = required_path(&args, "--private-key")?;
    let manifest = required_path(&args, "--manifest")?;
    validate_private_key_permissions(&private_key)?;
    let private_key = fs::read_to_string(private_key)?;
    let private_key = private_key.trim();
    if private_key.len() != 64 || !private_key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("private key must be a 64-character hex Ed25519 seed".into());
    }
    let mut key = [0_u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&private_key[index * 2..index * 2 + 2], 16)?;
    }
    let signing_key = SigningKey::from_bytes(&key);
    let input: Value = serde_json::from_slice(&fs::read(manifest)?)?;
    let manifest: UpgradeManifest =
        serde_json::from_value(input.get("manifest").cloned().unwrap_or(input))?;
    let signature = signing_key.sign(&serde_json::to_vec(&manifest)?);
    println!(
        "{}",
        serde_json::to_string_pretty(&SignedUpgradeManifest {
            manifest,
            signature: signature
                .to_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        })?
    );
    Ok(())
}

fn required_path(args: &[String], name: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| PathBuf::from(&pair[1]))
        .ok_or_else(|| format!("missing required option {name}").into())
}
