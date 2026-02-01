# getMangos Character Migration Tool (Web UI)

A self-contained **PHP 8.x** web tool for migrating character data between **MaNGOS Zero (Vanilla)** character databases. This project implements the migration logic in application code with a clean UI for selecting realms, previewing changes, and executing migrations.

## Current Support
- ✅ **MaNGOS Zero (Vanilla)** character databases **only**
- ❌ Not yet tested/validated for TBC/WotLK branches or other cores

> As additional cores are supported, the migration table set and rules may be expanded or made selectable by “core profile.”

---

## Features
- **Multiple character databases (“realms”)** selectable in the UI
- **Single-character migration** or **full migration (all characters)**
- **Preview step** shows the character snapshot and planned identifiers before execution
- **Collision-aware GUID mapping** (new GUID allocation per destination state)
- **Rename detection**: if the character name already exists in the destination DB, the tool forces rename on next login (`at_login` flag)
- **Per-character transactions** with **stop-on-first-failure** behavior for batch runs

---

## Requirements
- **PHP 8.1+** (PHP 8.2+ recommended)
- PHP extensions:
  - `pdo`
  - `pdo_mysql`
- **MySQL/MariaDB** user with:
  - read access to the **From** database
  - write access to the **To** database

---

## Installation / Running
1. Place the project in your web root (or any folder).
2. Copy configuration:
   - Duplicate `config/config.php.dist` → `config/config.php`
3. Edit `config/config.php` and define your character DB profiles (realms).
4. Start the development server:
   ```bash
   cd public
   php -S 127.0.0.1:8080
   ```
5. Open:
   - `http://127.0.0.1:8080`

---

## Security Notes (Important)
This tool is intended for **trusted/internal use** only.

Recommended protections:
- Run behind a firewall/VPN
- Add HTTP Basic Auth (or reverse proxy auth)
- Use a **least-privilege** DB user (do not reuse your production admin credentials)

---

## Compatibility & Assumptions
- The migration logic assumes the **destination schema matches the source schema** for all migrated tables.
- If your schema differs (missing/extra columns), inserts may fail.
- If you customize schemas, update the relevant insert/column lists in the migration service.

---

## Migrated Data
This tool migrates character-owned data from the **From** characters DB to the **To** characters DB.

Tables currently included (MaNGOS Zero Vanilla set):
- `characters`
- `corpse`
- `character_action`
- `character_aura`
- `character_battleground_data`
- `character_gifts`
- `character_homebind`
- `character_honor_cp`
- `character_instance`
- `character_inventory`
- `character_pet`
- `character_queststatus`
- `character_reputation`
- `character_skills`
- `character_spell`
- `character_spell_cooldown`
- `character_stats`
- `item_instance`
- `pet_aura`, `pet_spell`, `pet_spell_cooldown`

---

## Workflow Overview
1. Select **From** and **To** character databases (realms)
2. Choose:
   - **Single character** (select by name/GUID)  
   **or**
   - **All characters**
3. Review the **Preview**
4. Execute migration
5. Review the **Results** report

---

## Limitations (for now)
- Only supports **MaNGOS Zero (Vanilla)** at this time.
- Supports **MaNGOS One (TBC)** partially and needs testing.
- Not intended as a public-facing tool.
- Not optimized for massive scale; designed for accuracy and safety.

---

## License / Attribution
Add your project license information here (if applicable), or reference the repository license file.
