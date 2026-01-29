# Character Migration Web UI (PHP 8.x)

This is a small, self-contained PHP 8.x web application that **mimics** the provided
`character_migration_script.sql` behavior in application code.

## Features
- Multiple character databases (profiles) selectable in the UI
- Single-character migration or full (all-character) migration
- Preview page shows computed GUID translations (character GUID, item GUIDs, pet IDs)
- Per-character destination transaction, stop-on-first-failure in batch mode
- Professional UI using Bootstrap 5

## Requirements
- PHP 8.1+ recommended
- PDO MySQL extension enabled
- MariaDB/MySQL credentials with permissions to read source and write destination

## Install / Run
1. Copy the folder somewhere under your web root, or use the built-in server.
2. Edit `config/config.php` and configure your character DB profiles.
3. Start PHP built-in server (development):

```bash
cd public
php -S 127.0.0.1:8080
```

Then open `http://127.0.0.1:8080`.

## Security
This tool is intended for trusted/internal use. Protect it with:
- VPN/firewall restrictions
- HTTP Basic Auth (recommended)
- A least-privilege DB user

## Compatibility Notes
- The migration logic assumes the destination schema matches the source schema for the migrated tables.
- If your schema differs (extra/missing columns), insert statements will fail. Adjust column lists in `src/Migration/MigrationService.php`.
- For very large characters, this is implemented as fetch+insert loops (portable across different servers), which is accurate but may be slower than cross-schema `INSERT ... SELECT`.

## Migrated Tables
Matches the SQL script’s "Included" list:
- characters
- corpse
- character_action
- character_aura
- character_battleground_data
- character_gifts
- character_homebind
- character_honor_cp
- character_instance
- character_inventory
- character_pet
- character_queststatus
- character_reputation
- character_skills
- character_spell
- character_spell_cooldown
- character_stats
- item_instance
- pet_aura / pet_spell / pet_spell_cooldown
