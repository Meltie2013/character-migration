<?php

declare(strict_types=1);

namespace App\Migration;

use App\Db\ConnectionManager;
use App\Db\SchemaInspector;
use PDO;
use PDOStatement;
use Throwable;

final class MigrationService
{
    private const IN_CHUNK = 500;
    private const INSERT_CHUNK = 200;

    private ConnectionManager $connections;

    private SchemaInspector $schema;

    private int $sampleLimit;

    public function __construct(ConnectionManager $connections, int $sampleLimit = 10)
    {
        $this->connections = $connections;
        $this->schema = new SchemaInspector();
        $this->sampleLimit = max(1, $sampleLimit);
    }

    /**
        * List characters (for UI selection).
        *
        * @return array<int,array{guid:int, name:string, level:int, race:int, class:int, gender:int}>
        */
    public function listCharacters(string $sourceProfile, string $search = '', int $limit = 300): array
    {
        $src = $this->connections->getCharacter($sourceProfile);

        $limit = max(1, min(2000, $limit));
        $search = trim($search);

        if ($search !== '')
        {
            $isGuid = ctype_digit($search);
            $sql = "SELECT guid, name, level, race, class, gender FROM characters WHERE guid > 0 AND (name LIKE :like OR (:is_guid = 1 AND guid = :guid))
                    ORDER BY guid LIMIT :lim";

            $stmt = $src->prepare($sql);
            $stmt->bindValue(':like', '%' . $search . '%', PDO::PARAM_STR);
            $stmt->bindValue(':is_guid', $isGuid ? 1 : 0, PDO::PARAM_INT);
            $stmt->bindValue(':guid', $isGuid ? (int)$search : 0, PDO::PARAM_INT);
            $stmt->bindValue(':lim', $limit, PDO::PARAM_INT);
            $stmt->execute();

            return $stmt->fetchAll() ?: [];
        }

        $sql = "SELECT guid, name, level, race, class, gender FROM characters WHERE guid > 0 ORDER BY guid LIMIT :lim";
        $stmt = $src->prepare($sql);
        $stmt->bindValue(':lim', $limit, PDO::PARAM_INT);
        $stmt->execute();

        return $stmt->fetchAll() ?: [];
    }

    /**
        * Build a plan in single-character mode (no writes).
        */
    public function buildSinglePlan(string $sourceProfile, string $destProfile, int $srcGuid, int $dstGuidRequested = 0): MigrationPlan
    {
        if ($srcGuid <= 0)
        {
            throw new MigrationException('Source GUID must be > 0 for single-character mode.');
        }

        $src = $this->connections->getCharacter($sourceProfile);
        $dst = $this->connections->getCharacter($destProfile);

        // Validate source exists
        $exists = (int)$this->fetchOne($src, 'SELECT COUNT(*) FROM characters WHERE guid = :g', [':g' => $srcGuid]);
        if ($exists === 0)
        {
            throw new MigrationException('Source character GUID not found in source schema (characters table).');
        }

        $dstGuid = $dstGuidRequested > 0 ? $dstGuidRequested : $this->nextGuid($dst, 'characters', 'guid');

        // Ensure destination guid is free
        $taken = (int)$this->fetchOne($dst, 'SELECT COUNT(*) FROM characters WHERE guid = :g', [':g' => $dstGuid]);
        if ($taken > 0)
        {
            throw new MigrationException('Destination GUID already exists in destination schema (characters table).');
        }

        $plan = new MigrationPlan('single', $sourceProfile, $destProfile, $srcGuid, $dstGuidRequested);

        // UI-only: detect likely character DB structure for source/destination.
        $srcStruct = $this->schema->detectCharacterDbStructure($src);
        $dstStruct = $this->schema->detectCharacterDbStructure($dst);
        $plan->sourceSchemaLabel = (string)($srcStruct['label'] ?? 'Unknown');
        $plan->destSchemaLabel = (string)($dstStruct['label'] ?? 'Unknown');
        $plan->sourceSchemaSummary = (string)($srcStruct['summary'] ?? '');
        $plan->destSchemaSummary = (string)($dstStruct['summary'] ?? '');

        // Character preview (minimal fields)
        $char = $this->fetchRow($src, 'SELECT * FROM characters WHERE guid = :g', [':g' => $srcGuid]);
        $char['final_name'] = substr((string)($char['name'] ?? ''), 0, 12);
        $finalName = (string)$char['final_name'];
        $nameTaken = (int)$this->fetchOne($dst, 'SELECT COUNT(*) FROM characters WHERE name = :n', [':n' => $finalName]);
        $plan->forceRename = ($nameTaken > 0);

        $accountId = (int)($char['account'] ?? 0);
        $tutorialSrc = 0;
        $tutorialDst = 0;

        if ($accountId > 0)
        {
            $tutorialSrc = (int)$this->fetchOne($src, 'SELECT COUNT(*) FROM character_tutorial WHERE account = :a', [':a' => $accountId]);
            $tutorialDst = (int)$this->fetchOne($dst, 'SELECT COUNT(*) FROM character_tutorial WHERE account = :a', [':a' => $accountId]);
        }

        $char['tutorial_account'] = $accountId;
        $char['tutorial_src_exists'] = ($tutorialSrc > 0);
        $char['tutorial_dst_exists'] = ($tutorialDst > 0);
        $char['dst_guid'] = $dstGuid;
        $char['force_rename'] = $plan->forceRename;
        $plan->character = $char;

        // Build item map (src_item_guid => dst_item_guid)
        // NOTE: With MySQL/MariaDB PDO in native prepare mode, reusing the same named placeholder
        // multiple times inside a statement can raise:
        //   SQLSTATE[HY093]: Invalid parameter number
        // Therefore the item GUID query uses distinct parameter names, and we bind them all.
        [$itemSql, $itemParams] = $this->buildSourceItemGuidsQuery($src, $srcGuid);
        $itemGuids = $this->fetchColumnInts($src, $itemSql, $itemParams);

        sort($itemGuids);
        $itemBase = $this->nextGuid($dst, 'item_instance', 'guid');
        $map = [];
        $seq = $itemBase;
        foreach ($itemGuids as $ig)
        {
            $map[$ig] = $seq;
            $seq++;
        }

        $plan->itemMap = $map;

        // Diagnostics: item guids can be referenced by character tables even when the item_instance row is missing.
        // The SQL migration inserts item_instance via an inner JOIN, so those missing rows will not be created.
        // To make the UI less surprising, compute the found/missing counts for the preview.
        if ($map)
        {
            $refs = array_keys($map);
            sort($refs);
            $found = [];
            foreach (array_chunk($refs, self::IN_CHUNK) as $chunk)
            {
                $in = $this->placeholders(count($chunk));
                $stmt = $src->prepare("SELECT guid FROM item_instance WHERE guid IN ($in)");
                $this->bindIntList($stmt, $chunk);
                $stmt->execute();
                $rows = $stmt->fetchAll() ?: [];

                foreach ($rows as $r)
                {
                    $found[(int)$r['guid']] = true;
                }
            }

            $missing = [];
            foreach ($refs as $g)
            {
                if (!isset($found[$g]))
                {
                    $missing[] = $g;
                }
            }

            $plan->itemInstanceFound = count($found);
            $plan->itemInstanceMissing = count($missing);
            if ($missing)
            {
                sort($missing);
                $plan->missingItemInstanceSample = array_slice($missing, 0, $this->sampleLimit);
            }
        }

        // Build pet map (src_pet_id => dst_pet_id)
        $petIds = [];
        $pmap = [];

        if ($this->schema->tableExists($src, 'character_pet') && $this->schema->tableExists($dst, 'character_pet')
            && $this->schema->columnExists($src, 'character_pet', 'id') && $this->schema->columnExists($src, 'character_pet', 'owner')
            && $this->schema->columnExists($dst, 'character_pet', 'id'))
        {
            $petIds = $this->fetchColumnInts($src, 'SELECT id FROM character_pet WHERE owner = :g ORDER BY id', [':g' => $srcGuid]);
            $petBase = $this->nextGuid($dst, 'character_pet', 'id');

            $pseq = $petBase;
            foreach ($petIds as $pid)
            {
                $pmap[$pid] = $pseq;
                $pseq++;
            }
        }

        $plan->petMap = $pmap;

        return $plan;
    }

    /**
        * Build a plan in all-characters mode (no writes).
        */
    public function buildBatchPlan(string $sourceProfile, string $destProfile): MigrationPlan
    {
        $src = $this->connections->getCharacter($sourceProfile);
        $dst = $this->connections->getCharacter($destProfile);

        $plan = new MigrationPlan('all', $sourceProfile, $destProfile, 0, 0);

        // UI-only: detect likely character DB structure for source/destination.
        $srcStruct = $this->schema->detectCharacterDbStructure($src);
        $dstStruct = $this->schema->detectCharacterDbStructure($dst);
        $plan->sourceSchemaLabel = (string)($srcStruct['label'] ?? 'Unknown');
        $plan->destSchemaLabel = (string)($dstStruct['label'] ?? 'Unknown');
        $plan->sourceSchemaSummary = (string)($srcStruct['summary'] ?? '');
        $plan->destSchemaSummary = (string)($dstStruct['summary'] ?? '');

        $rows = $src->query('SELECT guid, name FROM characters WHERE guid > 0 ORDER BY guid')->fetchAll() ?: [];
        $next = $this->nextGuid($dst, 'characters', 'guid');

        // Preload destination names for collision detection (name is typically capped to 12 chars in-core).
        $dstNames = [];
        $dstRows = $dst->query('SELECT name FROM characters WHERE guid > 0')->fetchAll() ?: [];
        foreach ($dstRows as $dr)
        {
            $dn = (string)($dr['name'] ?? '');
            if ($dn !== '')
            {
                $dstNames[$dn] = true;
            }
        }

        foreach ($rows as $r)
        {
            $name = substr((string)$r['name'], 0, 12);
            $forceRename = isset($dstNames[$name]);

            $plan->batch[] = [
                'src_guid' => (int)$r['guid'],
                'dst_guid' => $next,
                'name' => $name,
                'force_rename' => $forceRename,
            ];
            $next++;
        }

        return $plan;
    }

    /**
        * Execute single-character migration based on a pre-built plan.
        */
    public function executeSingle(MigrationPlan $plan): MigrationResult
    {
        if ($plan->mode !== 'single')
        {
            throw new MigrationException('executeSingle requires a single-character plan.');
        }

        $src = $this->connections->getCharacter($plan->sourceProfile);
        $dst = $this->connections->getCharacter($plan->destProfile);

        $dstGuid = (int)($plan->character['dst_guid'] ?? 0);
        if ($dstGuid <= 0)
        {
            // If the plan did not include computed dst guid (should not happen), recompute.
            $dstGuid = $plan->dstGuidRequested > 0 ? $plan->dstGuidRequested : $this->nextGuid($dst, 'characters', 'guid');
        }

        $finalName = substr((string)($plan->character['final_name'] ?? ($plan->character['name'] ?? '')), 0, 12);

        $result = new MigrationResult();

        // UI-only: carry schema labels through to the results page.
        $result->sourceSchemaLabel = $plan->sourceSchemaLabel;
        $result->destSchemaLabel = $plan->destSchemaLabel;

        $this->migrateOne($src, $dst, $plan->srcGuid, $dstGuid, $finalName, $plan->itemMap, $plan->petMap, $plan->forceRename, $result);

        // Read final character from destination
        $result->finalCharacter = $this->fetchRow(
            $dst,
            'SELECT * FROM characters WHERE guid = :g',
            [':g' => $dstGuid]
        );

        $result->rows[] = [
            'src_guid' => $plan->srcGuid,
            'dst_guid' => $dstGuid,
            'name' => (string)($result->finalCharacter['name'] ?? $finalName),
            'status' => 'OK',
            'message' => $plan->forceRename ? 'Migrated successfully (rename forced on next login).' : 'Migrated successfully.',
        ];

        return $result;
    }

    /**
        * Execute all-character migration.
        */
    public function executeBatch(string $sourceProfile, string $destProfile): MigrationResult
    {
        $src = $this->connections->getCharacter($sourceProfile);
        $dst = $this->connections->getCharacter($destProfile);

        $result = new MigrationResult();

        // UI-only: detect likely character DB structure for source/destination.
        $srcStruct = $this->schema->detectCharacterDbStructure($src);
        $dstStruct = $this->schema->detectCharacterDbStructure($dst);
        $result->sourceSchemaLabel = (string)($srcStruct['label'] ?? 'Unknown');
        $result->destSchemaLabel = (string)($dstStruct['label'] ?? 'Unknown');

        $guids = $this->fetchColumnInts($src, 'SELECT guid FROM characters WHERE guid > 0 ORDER BY guid');
        foreach ($guids as $srcGuid)
        {
            try
            {
                $plan = $this->buildSinglePlan($sourceProfile, $destProfile, $srcGuid, 0);
                $dstGuid = (int)($plan->character['dst_guid'] ?? 0);
                $finalName = substr((string)($plan->character['final_name'] ?? ($plan->character['name'] ?? '')), 0, 12);

                $this->migrateOne($src, $dst, $srcGuid, $dstGuid, $finalName, $plan->itemMap, $plan->petMap, $plan->forceRename, $result);

                $result->rows[] = [
                    'src_guid' => $srcGuid,
                    'dst_guid' => $dstGuid,
                    'name' => $finalName,
                    'status' => 'OK',
                    'message' => $plan->forceRename ? 'Migrated successfully (rename forced on next login).' : 'Migrated successfully.',
                ];
            }
            catch (Throwable $e)
            {
                $result->rows[] = [
                    'src_guid' => $srcGuid,
                    'dst_guid' => 0,
                    'name' => '',
                    'status' => 'FAILED',
                    'message' => $e->getMessage(),
                ];

                // Mimic the SQL behavior: stop on first failure; previously migrated characters remain committed.
                break;
            }
        }

        return $result;
    }

    /**
        * Core migration logic that mimics character_migration_script.sql.
        *
        * @param array<int,int> $itemMap
        * @param array<int,int> $petMap
        */
    private function migrateOne(PDO $src, PDO $dst, int $srcGuid, int $dstGuid, string $finalName, array $itemMap, array $petMap, bool $forceRename, MigrationResult $result): void
    {
        // Safety: verify dst guid still free at execution time
        $taken = (int)$this->fetchOne($dst, 'SELECT COUNT(*) FROM characters WHERE guid = :g', [':g' => $dstGuid]);
        if ($taken > 0)
        {
            throw new MigrationException('Destination GUID already exists in destination schema (characters table).');
        }

        $dst->beginTransaction();
        $oldFk = (int)$this->fetchOne($dst, 'SELECT @@FOREIGN_KEY_CHECKS');

        try
        {
            $dst->exec('SET FOREIGN_KEY_CHECKS = 0');

            // characters
            $charCols = $this->resolveCommonColumns($src, $dst, 'characters', ['guid', 'account', 'name'], $result);
            if (!$charCols)
            {
                throw new MigrationException('Schema mismatch: unable to find compatible columns for characters table between source and destination.');
            }

            $char = $this->fetchRow($src, 'SELECT ' . $this->schema->selectList($charCols) . ' FROM `characters` WHERE guid = :g', [':g' => $srcGuid]);
            if (!$char)
            {
                throw new MigrationException('Source character GUID not found in source schema (characters table).');
            }

            $char['guid'] = $dstGuid;
            $char['name'] = $finalName;

            if ($forceRename && in_array('at_login', $charCols, true))
            {
                $char['at_login'] = ((int)($char['at_login'] ?? 0) | 1);
            }

            $this->insertOne($dst, 'characters', $charCols, $char, $result, 'characters');

            // character_tutorial (account-level tutorial completion flags)
            // This table is keyed by characters.account (account id). It is not character-owned by guid,
            // but it represents tutorial completion state for the account that owns this character.
            $accountId = (int) ($char['account'] ?? 0);
            if ($accountId > 0)
            {
                $this->migrateCharacterTutorial($src, $dst, $accountId, $result);
            }

            // corpse (set both corpse.guid and corpse.player to new guid)
            $corpseCols = $this->resolveCommonColumns($src, $dst, 'corpse', ['guid', 'player'], $result);
            if ($corpseCols)
            {
                $corpseRows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($corpseCols) . ' FROM `corpse` WHERE player = :g', [':g' => $srcGuid]);
                foreach ($corpseRows as &$r)
                {
                    if (array_key_exists('guid', $r))
                    {
                        $r['guid'] = $dstGuid;
                    }
                    if (array_key_exists('player', $r))
                    {
                        $r['player'] = $dstGuid;
                    }
                }

                unset($r);
                $this->bulkInsert($dst, 'corpse', $corpseCols, $corpseRows, $result, 'corpse');
            }

            // item_instance (for mapped items)
            $itemCols = $this->resolveCommonColumns($src, $dst, 'item_instance', ['guid', 'data'], $result);
            $srcItemGuids = array_keys($itemMap);
            sort($srcItemGuids);
            $missingItemInstances = [];

            if ($itemCols)
            {
                foreach (array_chunk($srcItemGuids, self::IN_CHUNK) as $chunk)
                {
                    $in = $this->placeholders(count($chunk));
                    $stmt = $src->prepare('SELECT ' . $this->schema->selectList($itemCols) . " FROM `item_instance` WHERE guid IN ($in)");
                    $this->bindIntList($stmt, $chunk);
                    $stmt->execute();
                    $items = $stmt->fetchAll() ?: [];

                    $found = [];
                    foreach ($items as $it)
                    {
                        $found[(int)$it['guid']] = true;
                    }

                    foreach ($chunk as $g)
                    {
                        if (!isset($found[$g]))
                        {
                            $missingItemInstances[] = $g;
                        }
                    }

                    $out = [];
                    foreach ($items as $it)
                    {
                        $sg = (int)$it['guid'];
                        if (!isset($itemMap[$sg]))
                        {
                            continue;
                        }

                        $row = [];
                        foreach ($itemCols as $c)
                        {
                            $row[$c] = $it[$c] ?? null;
                        }

                        $row['guid'] = $itemMap[$sg];
                        if (in_array('owner_guid', $itemCols, true))
                        {
                            $row['owner_guid'] = $dstGuid;
                        }

                        $out[] = $row;
                    }

                    $this->bulkInsert($dst, 'item_instance', $itemCols, $out, $result, 'item_instance');
                }
            }

            if ($missingItemInstances)
            {
                sort($missingItemInstances);
                $sample = array_slice($missingItemInstances, 0, 25);
                $result->warnings[] = sprintf(
                    'Detected %d mapped item GUID(s) referenced by the character that do not exist in source item_instance. This mirrors the SQL behavior (item_instance insert is a JOIN). Sample missing GUID(s): %s%s',
                    count($missingItemInstances),
                    implode(', ', $sample),
                    (count($missingItemInstances) > count($sample)) ? ' ...' : ''
                );
            }

            // character_inventory (map bag + item guids)
            $invCols = $this->resolveCommonColumns($src, $dst, 'character_inventory', ['guid', 'slot', 'item'], $result);
            if ($invCols)
            {
                $inv = $this->fetchAll($src, 'SELECT bag, slot, item, item_template FROM `character_inventory` WHERE guid = :g', [':g' => $srcGuid]);
                $invOut = [];

                foreach ($inv as $row)
                {
                    $item = (int)$row['item'];
                    $bag = (int)$row['bag'];

                    if ($item === 0 || !isset($itemMap[$item]))
                    {
                        continue;
                    }

                    $outRow = [];
                    foreach ($invCols as $c)
                    {
                        if ($c === 'guid')
                        {
                            $outRow['guid'] = $dstGuid;
                            continue;
                        }

                        if ($c === 'bag')
                        {
                            $outRow['bag'] = ($bag === 0) ? 0 : ($itemMap[$bag] ?? 0);
                            continue;
                        }

                        if ($c === 'slot')
                        {
                            $outRow['slot'] = (int)$row['slot'];
                            continue;
                        }

                        if ($c === 'item')
                        {
                            $outRow['item'] = $itemMap[$item];
                            continue;
                        }

                        if ($c === 'item_template')
                        {
                            $outRow['item_template'] = (int)$row['item_template'];
                            continue;
                        }

                        // For any additional columns, fall back to source row if present, otherwise NULL.
                        $outRow[$c] = $row[$c] ?? null;
                    }

                    $invOut[] = $outRow;
                }

                $this->bulkInsert($dst, 'character_inventory', $invCols, $invOut, $result, 'character_inventory');
            }

            // character_action
            $cols = $this->resolveCommonColumns($src, $dst, 'character_action', ['guid', 'button', 'action', 'type'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_action` WHERE guid = :g', [':g' => $srcGuid]);
                $this->bulkInsertGuided($dstGuid, $dst, 'character_action', $cols, $rows, $result, 'character_action');
            }

            // character_aura (clear item_guid to avoid migrating stale item attribution GUIDs)
            $cols = $this->resolveCommonColumns($src, $dst, 'character_aura', ['guid', 'spell'], $result);
            if ($cols)
            {
                $sel = $cols;
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_aura` WHERE guid = :g', [':g' => $srcGuid]);
                $out = [];

                foreach ($rows as $r)
                {
                    $row = [];
                    foreach ($cols as $c)
                    {
                        if ($c === 'guid')
                        {
                            $row['guid'] = $dstGuid;
                            continue;
                        }

                        if ($c === 'item_guid')
                        {
                            $row['item_guid'] = 0;
                            continue;
                        }

                        $row[$c] = $r[$c] ?? null;
                    }

                    $out[] = $row;
                }

                $this->bulkInsert($dst, 'character_aura', $cols, $out, $result, 'character_aura');
            }

            // character_battleground_data
            $cols = $this->resolveCommonColumns($src, $dst, 'character_battleground_data', ['guid', 'instance_id'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_battleground_data` WHERE guid = :g', [':g' => $srcGuid]);
                $this->bulkInsertGuided($dstGuid, $dst, 'character_battleground_data', $cols, $rows, $result, 'character_battleground_data');
            }

            // character_gifts (map item_guid)
            $cols = $this->resolveCommonColumns($src, $dst, 'character_gifts', ['guid', 'item_guid'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_gifts` WHERE guid = :g', [':g' => $srcGuid]);

                $out = [];
                foreach ($rows as $r)
                {
                    $ig = (int)($r['item_guid'] ?? 0);
                    if ($ig === 0 || !isset($itemMap[$ig]))
                    {
                        continue;
                    }

                    $row = [];
                    foreach ($cols as $c)
                    {
                        if ($c === 'guid')
                        {
                            $row['guid'] = $dstGuid;
                            continue;
                        }

                        if ($c === 'item_guid')
                        {
                            $row['item_guid'] = $itemMap[$ig];
                            continue;
                        }

                        $row[$c] = $r[$c] ?? null;
                    }

                    $out[] = $row;
                }

                $this->bulkInsert($dst, 'character_gifts', $cols, $out, $result, 'character_gifts');
            }

            // character_homebind
            $cols = $this->resolveCommonColumns($src, $dst, 'character_homebind', ['guid', 'map'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_homebind` WHERE guid = :g', [':g' => $srcGuid]);
                $this->bulkInsertGuided($dstGuid, $dst, 'character_homebind', $cols, $rows, $result, 'character_homebind');
            }

            // character_honor_cp
            $cols = $this->resolveCommonColumns($src, $dst, 'character_honor_cp', ['guid'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_honor_cp` WHERE guid = :g', [':g' => $srcGuid]);
                $this->bulkInsertGuided($dstGuid, $dst, 'character_honor_cp', $cols, $rows, $result, 'character_honor_cp');
            }

            // character_instance
            $cols = $this->resolveCommonColumns($src, $dst, 'character_instance', ['guid', 'instance'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_instance` WHERE guid = :g', [':g' => $srcGuid]);
                $this->bulkInsertGuided($dstGuid, $dst, 'character_instance', $cols, $rows, $result, 'character_instance');
            }

            // character_queststatus
            $cols = $this->resolveCommonColumns($src, $dst, 'character_queststatus', ['guid', 'quest'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_queststatus` WHERE guid = :g', [':g' => $srcGuid]);
                $this->bulkInsertGuided($dstGuid, $dst, 'character_queststatus', $cols, $rows, $result, 'character_queststatus');
            }

            // character_reputation
            $cols = $this->resolveCommonColumns($src, $dst, 'character_reputation', ['guid', 'faction'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_reputation` WHERE guid = :g', [':g' => $srcGuid]);
                $this->bulkInsertGuided($dstGuid, $dst, 'character_reputation', $cols, $rows, $result, 'character_reputation');
            }

            // character_skills
            $cols = $this->resolveCommonColumns($src, $dst, 'character_skills', ['guid', 'skill'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_skills` WHERE guid = :g', [':g' => $srcGuid]);
                $this->bulkInsertGuided($dstGuid, $dst, 'character_skills', $cols, $rows, $result, 'character_skills');
            }

            // character_spell
            $cols = $this->resolveCommonColumns($src, $dst, 'character_spell', ['guid', 'spell'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_spell` WHERE guid = :g', [':g' => $srcGuid]);
                $this->bulkInsertGuided($dstGuid, $dst, 'character_spell', $cols, $rows, $result, 'character_spell');
            }

            // character_spell_cooldown
            $cols = $this->resolveCommonColumns($src, $dst, 'character_spell_cooldown', ['guid', 'spell'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_spell_cooldown` WHERE guid = :g', [':g' => $srcGuid]);
                $this->bulkInsertGuided($dstGuid, $dst, 'character_spell_cooldown', $cols, $rows, $result, 'character_spell_cooldown');
            }

            // character_stats
            $cols = $this->resolveCommonColumns($src, $dst, 'character_stats', ['guid', 'maxhealth'], $result);
            if ($cols)
            {
                $sel = array_values(array_filter($cols, static fn($c) => $c !== 'guid'));
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($sel) . ' FROM `character_stats` WHERE guid = :g', [':g' => $srcGuid]);
                $this->bulkInsertGuided($dstGuid, $dst, 'character_stats', $cols, $rows, $result, 'character_stats');
            }

            // character_pet (map pet IDs; owner becomes dst guid)
            $cols = $this->resolveCommonColumns($src, $dst, 'character_pet', ['id', 'owner'], $result);
            if ($cols)
            {
                $rows = $this->fetchAll($src, 'SELECT ' . $this->schema->selectList($cols) . ' FROM `character_pet` WHERE owner = :g ORDER BY id', [':g' => $srcGuid]);
                $out = [];

                foreach ($rows as $r)
                {
                    $sid = (int)($r['id'] ?? 0);
                    if ($sid <= 0 || !isset($petMap[$sid]))
                    {
                        continue;
                    }

                    $row = [];
                    foreach ($cols as $c)
                    {
                        if ($c === 'id')
                        {
                            $row['id'] = $petMap[$sid];
                            continue;
                        }

                        if ($c === 'owner')
                        {
                            $row['owner'] = $dstGuid;
                            continue;
                        }

                        $row[$c] = $r[$c] ?? null;
                    }

                    $out[] = $row;
                }

                $this->bulkInsert($dst, 'character_pet', $cols, $out, $result, 'character_pet');
            }

            // pet_spell
            $petSpellCols = $this->resolveCommonColumns($src, $dst, 'pet_spell', ['guid', 'spell'], $result);
            if ($petSpellCols)
            {
                $petIdsSrc = array_keys($petMap);
                sort($petIdsSrc);

                foreach (array_chunk($petIdsSrc, self::IN_CHUNK) as $chunk)
                {
                    $in = $this->placeholders(count($chunk));
                    $stmt = $src->prepare('SELECT ' . $this->schema->selectList($petSpellCols) . " FROM `pet_spell` WHERE guid IN ($in)");
                    $this->bindIntList($stmt, $chunk);
                    $stmt->execute();
                    $rows = $stmt->fetchAll() ?: [];

                    $out = [];
                    foreach ($rows as $r)
                    {
                        $sg = (int)($r['guid'] ?? 0);
                        if ($sg <= 0 || !isset($petMap[$sg]))
                        {
                            continue;
                        }

                        $row = [];
                        foreach ($petSpellCols as $c)
                        {
                            if ($c === 'guid')
                            {
                                $row['guid'] = $petMap[$sg];
                                continue;
                            }

                            $row[$c] = $r[$c] ?? null;
                        }

                        $out[] = $row;
                    }

                    $this->bulkInsert($dst, 'pet_spell', $petSpellCols, $out, $result, 'pet_spell');
                }
            }

            // pet_spell_cooldown
            $petCdCols = $this->resolveCommonColumns($src, $dst, 'pet_spell_cooldown', ['guid', 'spell'], $result);
            if ($petCdCols)
            {
                $petIdsSrc = array_keys($petMap);
                sort($petIdsSrc);

                foreach (array_chunk($petIdsSrc, self::IN_CHUNK) as $chunk)
                {
                    $in = $this->placeholders(count($chunk));
                    $stmt = $src->prepare('SELECT ' . $this->schema->selectList($petCdCols) . " FROM `pet_spell_cooldown` WHERE guid IN ($in)");
                    $this->bindIntList($stmt, $chunk);
                    $stmt->execute();
                    $rows = $stmt->fetchAll() ?: [];
                    $out = [];

                    foreach ($rows as $r)
                    {
                        $sg = (int)($r['guid'] ?? 0);
                        if ($sg <= 0 || !isset($petMap[$sg]))
                        {
                            continue;
                        }

                        $row = [];
                        foreach ($petCdCols as $c)
                        {
                            if ($c === 'guid')
                            {
                                $row['guid'] = $petMap[$sg];
                                continue;
                            }

                            $row[$c] = $r[$c] ?? null;
                        }

                        $out[] = $row;
                    }

                    $this->bulkInsert($dst, 'pet_spell_cooldown', $petCdCols, $out, $result, 'pet_spell_cooldown');
                }
            }

            // pet_aura (map pet guid; clear item_guid)
            $petAuraCols = $this->resolveCommonColumns($src, $dst, 'pet_aura', ['guid', 'spell'], $result);
            if ($petAuraCols)
            {
                $petIdsSrc = array_keys($petMap);
                sort($petIdsSrc);

                foreach (array_chunk($petIdsSrc, self::IN_CHUNK) as $chunk)
                {
                    $in = $this->placeholders(count($chunk));
                    $stmt = $src->prepare('SELECT ' . $this->schema->selectList($petAuraCols) . " FROM `pet_aura` WHERE guid IN ($in)");
                    $this->bindIntList($stmt, $chunk);
                    $stmt->execute();
                    $rows = $stmt->fetchAll() ?: [];
                    $out = [];

                    foreach ($rows as $r)
                    {
                        $sg = (int)($r['guid'] ?? 0);
                        if ($sg <= 0 || !isset($petMap[$sg]))
                        {
                            continue;
                        }

                        $row = [];
                        foreach ($petAuraCols as $c)
                        {
                            if ($c === 'guid')
                            {
                                $row['guid'] = $petMap[$sg];
                                continue;
                            }

                            if ($c === 'item_guid')
                            {
                                $row['item_guid'] = 0;
                                continue;
                            }

                            $row[$c] = $r[$c] ?? null;
                        }

                        $out[] = $row;
                    }

                    $this->bulkInsert($dst, 'pet_aura', $petAuraCols, $out, $result, 'pet_aura');
                }
            }

            $dst->commit();
        }
        catch (Throwable $e)
        {
            if ($dst->inTransaction())
            {
                $dst->rollBack();
            }

            throw $e;
        }
        finally
        {
            try
            {
                $dst->exec('SET FOREIGN_KEY_CHECKS = ' . $oldFk);
            }
            catch (Throwable $ignored)
            {
                // ignore
            }
        }
    }


    /**
     * Migrate tutorial completion data for the owning account (character_tutorial).
     *
     * The tutorial table is keyed by account id (characters.account). If a row already exists
     * in the destination, tutorial flags are merged using bitwise OR to preserve completion.
     */
    private function migrateCharacterTutorial(PDO $src, PDO $dst, int $accountId, MigrationResult $result): void
    {
        $cols = $this->resolveCommonColumns($src, $dst, 'character_tutorial', ['account'], $result);
        if (!$cols)
        {
            return;
        }

        $row = $this->fetchRow(
            $src,
            'SELECT ' . $this->schema->selectList($cols) . ' FROM `character_tutorial` WHERE account = :a',
            [':a' => $accountId]
        );

        if (!$row)
        {
            return;
        }

        $exists = (int) $this->fetchOne($dst, 'SELECT COUNT(*) FROM `character_tutorial` WHERE account = :a', [':a' => $accountId]);
        if ($exists > 0)
        {
            $set = [];
            foreach ($cols as $c)
            {
                if ($c === 'account')
                {
                    continue;
                }

                $set[] = '`' . $c . '` = (`' . $c . '` | :' . $c . ')';
            }

            if (!$set)
            {
                return;
            }

            $sql = 'UPDATE `character_tutorial` SET ' . implode(', ', $set) . ' WHERE account = :a';
            $stmt = $dst->prepare($sql);
            $stmt->bindValue(':a', $accountId, PDO::PARAM_INT);

            foreach ($cols as $c)
            {
                if ($c === 'account')
                {
                    continue;
                }

                $stmt->bindValue(':' . $c, (int) ($row[$c] ?? 0), PDO::PARAM_INT);
            }

            $stmt->execute();

            $result->counts['character_tutorial'] = ($result->counts['character_tutorial'] ?? 0) + $stmt->rowCount();
            return;
        }

        $this->insertOne($dst, 'character_tutorial', $cols, $row, $result, 'character_tutorial');
    }

    /** @return array<string,mixed> */
    private function fetchRow(PDO $pdo, string $sql, array $params = []): array
    {
        $stmt = $pdo->prepare($sql);
        foreach ($params as $k => $v) {
            $stmt->bindValue($k, $v, is_int($v) ? PDO::PARAM_INT : PDO::PARAM_STR);
        }

        $stmt->execute();
        $row = $stmt->fetch();
        return is_array($row) ? $row : [];
    }

    /** @return array<int,array<string,mixed>> */
    private function fetchAll(PDO $pdo, string $sql, array $params = []): array
    {
        $stmt = $pdo->prepare($sql);
        foreach ($params as $k => $v)
        {
            $stmt->bindValue($k, $v, is_int($v) ? PDO::PARAM_INT : PDO::PARAM_STR);
        }

        $stmt->execute();
        return $stmt->fetchAll() ?: [];
    }

    private function fetchOne(PDO $pdo, string $sql, array $params = []): string
    {
        $stmt = $pdo->prepare($sql);
        foreach ($params as $k => $v)
        {
            $stmt->bindValue($k, $v, is_int($v) ? PDO::PARAM_INT : PDO::PARAM_STR);
        }

        $stmt->execute();
        $val = $stmt->fetchColumn();
        return ($val === false || $val === null) ? '0' : (string)$val;
    }

    /** @return array<int,int> */
    private function fetchColumnInts(PDO $pdo, string $sql, array $params = []): array
    {
        $stmt = $pdo->prepare($sql);
        foreach ($params as $k => $v)
        {
            $stmt->bindValue($k, $v, is_int($v) ? PDO::PARAM_INT : PDO::PARAM_STR);
        }

        $stmt->execute();
        $out = [];
        while (($v = $stmt->fetchColumn()) !== false)
        {
            $out[] = (int)$v;
        }

        return $out;
    }

    private function nextGuid(PDO $pdo, string $table, string $col): int
    {
        $t = $this->q($table);
        $c = $this->q($col);

        $max = (int)$this->fetchOne($pdo, "SELECT COALESCE(MAX($c),0) FROM $t");
        return $max + 1;
    }

    private function q(string $name): string
    {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $name))
        {
            throw new \InvalidArgumentException('Invalid identifier: ' . $name);
        }

        return '`' . $name . '`';
    }

    /**
        * @param array<int,string> $required
        * @return array<int,string>
        */
    private function resolveCommonColumns(PDO $src, PDO $dst, string $table, array $required, MigrationResult $result): array
    {
        $cols = $this->schema->commonColumns($src, $dst, $table, $required);
        if ($cols)
        {
            return $cols;
        }

        $srcHas = $this->schema->tableExists($src, $table);
        $dstHas = $this->schema->tableExists($dst, $table);

        if ($srcHas || $dstHas)
        {
            if (!$srcHas)
            {
                $result->warnings[] = "Skipped table '$table': not present in source schema.";
            }
            elseif (!$dstHas)
            {
                $result->warnings[] = "Skipped table '$table': not present in destination schema.";
            }
            else
            {
                $result->warnings[] = "Skipped table '$table': compatible column set could not be resolved between source and destination schemas.";
            }
        }

        return [];
    }

    /**
        * Build an item GUID reference query based on tables/columns that exist in the source schema.
        *
        * @return array{0:string,1:array<string,int>}
        */
    private function buildSourceItemGuidsQuery(PDO $src, int $srcGuid): array
    {
        $parts = [];
        $params = [];

        if ($this->schema->tableExists($src, 'character_inventory')
            && $this->schema->columnExists($src, 'character_inventory', 'guid')
            && $this->schema->columnExists($src, 'character_inventory', 'item'))
        {
            $parts[] = 'SELECT DISTINCT item AS src_item_guid FROM `character_inventory` WHERE guid = :g1 AND item <> 0';
            $params[':g1'] = $srcGuid;
        }

        if ($this->schema->tableExists($src, 'character_inventory')
            && $this->schema->columnExists($src, 'character_inventory', 'guid')
            && $this->schema->columnExists($src, 'character_inventory', 'bag'))
        {
            $parts[] = 'SELECT DISTINCT bag AS src_item_guid FROM `character_inventory` WHERE guid = :g2 AND bag <> 0';
            $params[':g2'] = $srcGuid;
        }

        if ($this->schema->tableExists($src, 'character_gifts')
            && $this->schema->columnExists($src, 'character_gifts', 'guid')
            && $this->schema->columnExists($src, 'character_gifts', 'item_guid'))
        {
            $parts[] = 'SELECT DISTINCT item_guid AS src_item_guid FROM `character_gifts` WHERE guid = :g3 AND item_guid <> 0';
            $params[':g3'] = $srcGuid;
        }

        if (!$parts)
        {
            return ['SELECT 0 AS src_item_guid WHERE 1 = 0', []];
        }

        $sql = implode("\n            UNION\n            ", $parts) . "\n            ORDER BY src_item_guid";
        return [$sql, $params];
    }

private function insertOne(PDO $dst, string $table, array $columns, array $row, MigrationResult $result, string $countKey): void
    {
        $out = [];
        foreach ($columns as $c)
        {
            $out[$c] = $row[$c] ?? null;
        }

        $this->bulkInsert($dst, $table, $columns, [$out], $result, $countKey);
    }

    /**
        * @param array<int,array<string,mixed>> $rows
        */
    private function bulkInsertGuided(int $dstGuid, PDO $dst, string $table, array $columns, array $rows, MigrationResult $result, string $countKey): void
    {
        if (!$rows)
        {
            return;
        }

        $out = [];
        foreach ($rows as $r)
        {
            $row = ['guid' => $dstGuid];
            foreach ($columns as $c)
            {
                if ($c === 'guid')
                {
                    continue;
                }

                $row[$c] = $r[$c] ?? null;
            }

            $out[] = $row;
        }
        $this->bulkInsert($dst, $table, $columns, $out, $result, $countKey);
    }

    /**
        * @param array<int,string> $columns
        * @param array<int,array<string,mixed>> $rows
        */
    private function bulkInsert(PDO $pdo, string $table, array $columns, array $rows, MigrationResult $result, string $countKey): void
    {
        if (!$rows)
        {
            return;
        }

        foreach (array_chunk($rows, self::INSERT_CHUNK) as $chunk)
        {
            $place = '(' . implode(', ', array_fill(0, count($columns), '?')) . ')';
            $sql = 'INSERT INTO ' . $table . ' (' . implode(', ', $columns) . ') VALUES ' . implode(', ', array_fill(0, count($chunk), $place));
            $stmt = $pdo->prepare($sql);

            $i = 1;
            foreach ($chunk as $row)
            {
                foreach ($columns as $c)
                {
                    $v = $row[$c] ?? null;
                    if (is_int($v))
                    {
                        $stmt->bindValue($i, $v, PDO::PARAM_INT);
                    }
                    elseif ($v === null)
                    {
                        $stmt->bindValue($i, null, PDO::PARAM_NULL);
                    }
                    else
                    {
                        $stmt->bindValue($i, $v, PDO::PARAM_STR);
                    }

                    $i++;
                }
            }

            $stmt->execute();
            $result->counts[$countKey] = ($result->counts[$countKey] ?? 0) + $stmt->rowCount();
        }
    }

    private function placeholders(int $n): string
    {
        return implode(', ', array_fill(0, $n, '?'));
    }

    /** @param array<int,int> $vals */
    private function bindIntList(PDOStatement $stmt, array $vals): void
    {
        $i = 1;
        foreach ($vals as $v)
        {
            $stmt->bindValue($i, (int)$v, PDO::PARAM_INT);
            $i++;
        }
    }
}
