use libsabledb::metadata;
use libsabledb::storage::{StorageAdapter, StorageOpenParams, DB_NO_VERSION, DB_VERSION_1_0_0};
use libsabledb::BytesMutUtils;
use std::path::PathBuf;

#[derive(clap::Args)]
pub struct UpgradeOptions {
    dbpath: PathBuf,
}

type UpgradeFunc = fn(&mut StorageAdapter) -> Result<(), libsabledb::SableError>;

lazy_static::lazy_static! {
    static ref UPGRADE_PATH:
        Vec<(&'static str, UpgradeFunc)> =
            vec![
                // from version : upgrade function
                (DB_NO_VERSION, upgrade_from_noversion_to_1_0_0)
            ];
}

#[allow(dead_code)]
fn upgrade_from_noversion_to_1_0_0(
    _store: &mut StorageAdapter,
) -> Result<(), libsabledb::SableError> {
    tracing::info!(
        "Starting database upgrade from: {} -> {}",
        DB_NO_VERSION,
        DB_VERSION_1_0_0
    );
    Ok(())
}

pub fn upgrade_database(options: &UpgradeOptions) -> Result<(), libsabledb::SableError> {
    tracing::info!("Upgrading database...");
    // Check the current database version
    let open_params = StorageOpenParams::default().set_path(&options.dbpath);
    let mut store = StorageAdapter::default();
    store.open(open_params)?;

    let version_key = metadata::db_version_key();
    let db_version = if !store.contains(&version_key)? {
        DB_NO_VERSION.to_string()
    } else {
        BytesMutUtils::to_string(
            &store
                .get(&version_key)?
                .ok_or(libsabledb::SableError::NotFound)?,
        )
    };

    // locate the first function to call
    let iter = UPGRADE_PATH.iter();
    let mut found = false;
    for (from_version, func) in iter {
        if !found {
            if db_version.eq(from_version) {
                found = true;
                // fall through
            } else {
                continue;
            }
        }

        func(&mut store)?;
    }
    tracing::info!("Upgrading database...success");
    Ok(())
}
