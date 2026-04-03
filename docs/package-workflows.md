# Package Workflows

Two distribution paths are supported in this repository.

## Public Export

Use this when you want to share the project without secrets or runtime data.

```powershell
.\scripts\export-public.ps1
```

Output:

* `artifacts/public/chirpstack-docker-public-YYYYMMDD-HHMMSS.zip`

Included:

* Source files
* Docker Compose files
* Configuration files
* `dashboard/.env.example`

Excluded:

* `dashboard/.env`
* PostgreSQL dumps
* SQLite database files
* Redis dump files
* Existing backup artifacts

## Private Migration Package

Use this when you want to move your current environment to another machine.

```powershell
.\scripts\backup-private.ps1
```

Output:

* `artifacts/private/chirpstack-docker-private-YYYYMMDD-HHMMSS.zip`

The package contains:

* A safe project snapshot
* `migration-data/dashboard.env`
* `migration-data/chirpstack.sql`
* `migration-data/lora_history.db`
* `migration-data/dump.rdb`

This package is private and must not be uploaded to GitHub or shared publicly.

## Restore On Another Machine

1. Extract the private migration zip.
2. Open PowerShell in the extracted project folder.
3. Run:

```powershell
.\scripts\restore-private.ps1
```

By default, the restore script resets the named Docker volumes before loading
the backup so the restored environment matches the source machine.
