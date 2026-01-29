<?php

declare(strict_types=1);

namespace App\Migration;

use App\Db\ConnectionManager;
use PDO;
use PDOStatement;
use Throwable;

final class MigrationService
{
    private const IN_CHUNK = 500;
    private const INSERT_CHUNK = 200;

    private ConnectionManager $connections;

    private int $sampleLimit;

    public function __construct(ConnectionManager $connections, int $sampleLimit = 10)
    {
        $this->connections = $connections;
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
        // Therefore sqlSourceItemGuids() uses distinct parameter names, and we bind them all.
        $itemGuids = $this->fetchColumnInts($src, $this->sqlSourceItemGuids(), [
            ':g1' => $srcGuid,
            ':g2' => $srcGuid,
            ':g3' => $srcGuid,
            ]);

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
        $petIds = $this->fetchColumnInts($src, 'SELECT id FROM character_pet WHERE owner = :g ORDER BY id', [':g' => $srcGuid]);
        $petBase = $this->nextGuid($dst, 'character_pet', 'id');
        $pmap = [];
        $pseq = $petBase;
        foreach ($petIds as $pid)
        {
            $pmap[$pid] = $pseq;
            $pseq++;
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
            $char = $this->fetchRow($src, $this->sqlCharactersSelect(), [':g' => $srcGuid]);
            if (!$char)
            {
                throw new MigrationException('Source character GUID not found in source schema (characters table).');
            }

            $char['guid'] = $dstGuid;
            $char['name'] = $finalName;

            if ($forceRename)
            {
                $char['at_login'] = ((int)($char['at_login'] ?? 0) | 1);
            }

            $this->insertOne($dst, 'characters', $this->charactersColumns(), $char, $result, 'characters');

            // character_tutorial (account-level tutorial completion flags)
            // This table is keyed by characters.account (account id). It is not character-owned by guid,
            // but it represents tutorial completion state for the account that owns this character.
            $accountId = (int) ($char['account'] ?? 0);
            if ($accountId > 0)
            {
                $this->migrateCharacterTutorial($src, $dst, $accountId, $result);
            }

            // corpse (set both corpse.guid and corpse.player to new guid)
            $corpseRows = $this->fetchAll($src, 'SELECT guid, player, position_x, position_y, position_z, orientation, map, time, corpse_type, instance FROM corpse WHERE player = :g', [':g' => $srcGuid]);
            foreach ($corpseRows as &$r)
            {
                $r['guid'] = $dstGuid;
                $r['player'] = $dstGuid;
            }

            unset($r);
            $this->bulkInsert($dst, 'corpse', ['guid', 'player', 'position_x', 'position_y', 'position_z', 'orientation', 'map', 'time', 'corpse_type', 'instance'], $corpseRows, $result, 'corpse');

            // item_instance (for mapped items)
            $srcItemGuids = array_keys($itemMap);
            sort($srcItemGuids);
            $missingItemInstances = [];
            foreach (array_chunk($srcItemGuids, self::IN_CHUNK) as $chunk)
            {
                $in = $this->placeholders(count($chunk));
                $stmt = $src->prepare("SELECT guid, data, text FROM item_instance WHERE guid IN ($in)");
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

                    $out[] = [
                        'guid' => $itemMap[$sg],
                        'owner_guid' => $dstGuid,
                        'data' => $it['data'],
                        'text' => $it['text'],
                    ];
                }

                $this->bulkInsert($dst, 'item_instance', ['guid', 'owner_guid', 'data', 'text'], $out, $result, 'item_instance');
            }

            if ($missingItemInstances)
            {
                sort($missingItemInstances);
                $sample = array_slice($missingItemInstances, 0, 25);
                $result->warnings[] = sprintf(
                    'Detected %d mapped item GUID(s) referenced by the character that do not exist in source item_instance. This mirrors the SQL behavior (item_instance insert is a JOIN). Sample missing GUID(s): %s%s',
                    count($missingItemInstances),
                    implode(', ', $sample),
                    count($missingItemInstances) > count($sample) ? ' ...' : ''
                );
            }

            // character_inventory (map bag + item guids)
            $inv = $this->fetchAll($src, 'SELECT bag, slot, item, item_template FROM character_inventory WHERE guid = :g', [':g' => $srcGuid]);
            $invOut = [];
            foreach ($inv as $row)
            {
                $item = (int)$row['item'];
                $bag = (int)$row['bag'];
                if ($item === 0 || !isset($itemMap[$item]))
                {
                    continue;
                }

                $invOut[] = [
                    'guid' => $dstGuid,
                    'bag' => ($bag === 0) ? 0 : ($itemMap[$bag] ?? 0),
                    'slot' => (int)$row['slot'],
                    'item' => $itemMap[$item],
                    'item_template' => (int)$row['item_template'],
                ];
            }

            $this->bulkInsert($dst, 'character_inventory', ['guid', 'bag', 'slot', 'item', 'item_template'], $invOut, $result, 'character_inventory');

            // character_action
            $rows = $this->fetchAll($src, 'SELECT button, action, type FROM character_action WHERE guid = :g', [':g' => $srcGuid]);
            $this->bulkInsertGuided($dstGuid, $dst, 'character_action', ['guid', 'button', 'action', 'type'], $rows, $result, 'character_action');

            // character_aura (clear item_guid to avoid migrating stale item attribution GUIDs)
            $rows = $this->fetchAll($src, 'SELECT caster_guid, item_guid, spell, stackcount, remaincharges, basepoints0, basepoints1, basepoints2, periodictime0, periodictime1, periodictime2, maxduration, remaintime, effIndexMask FROM character_aura WHERE guid = :g', [':g' => $srcGuid]);
            $out = [];
            foreach ($rows as $r)
            {
                $ig = (int)$r['item_guid'];
                $out[] = [
                    'guid' => $dstGuid,
                    'caster_guid' => (int)$r['caster_guid'],
                    'item_guid' => 0,
                    'spell' => (int)$r['spell'],
                    'stackcount' => (int)$r['stackcount'],
                    'remaincharges' => (int)$r['remaincharges'],
                    'basepoints0' => (int)$r['basepoints0'],
                    'basepoints1' => (int)$r['basepoints1'],
                    'basepoints2' => (int)$r['basepoints2'],
                    'periodictime0' => (int)$r['periodictime0'],
                    'periodictime1' => (int)$r['periodictime1'],
                    'periodictime2' => (int)$r['periodictime2'],
                    'maxduration' => (int)$r['maxduration'],
                    'remaintime' => (int)$r['remaintime'],
                    'effIndexMask' => (int)$r['effIndexMask'],
                ];
            }

            $this->bulkInsert($dst, 'character_aura', ['guid', 'caster_guid', 'item_guid', 'spell', 'stackcount', 'remaincharges', 'basepoints0', 'basepoints1', 'basepoints2', 'periodictime0', 'periodictime1', 'periodictime2', 'maxduration', 'remaintime', 'effIndexMask'], $out, $result, 'character_aura');

            // character_battleground_data
            $rows = $this->fetchAll($src, 'SELECT instance_id, team, join_x, join_y, join_z, join_o, join_map FROM character_battleground_data WHERE guid = :g', [':g' => $srcGuid]);
            $this->bulkInsertGuided($dstGuid, $dst, 'character_battleground_data', ['guid', 'instance_id', 'team', 'join_x', 'join_y', 'join_z', 'join_o', 'join_map'], $rows, $result, 'character_battleground_data');

            // character_gifts (map item_guid)
            $rows = $this->fetchAll($src, 'SELECT item_guid, entry, flags FROM character_gifts WHERE guid = :g', [':g' => $srcGuid]);
            $out = [];
            foreach ($rows as $r)
            {
                $ig = (int)$r['item_guid'];
                if ($ig === 0 || !isset($itemMap[$ig]))
                {
                    continue;
                }

                $out[] = [
                    'guid' => $dstGuid,
                    'item_guid' => $itemMap[$ig],
                    'entry' => (int)$r['entry'],
                    'flags' => (int)$r['flags'],
                ];
            }

            $this->bulkInsert($dst, 'character_gifts', ['guid', 'item_guid', 'entry', 'flags'], $out, $result, 'character_gifts');

            // character_homebind
            $rows = $this->fetchAll($src, 'SELECT map, zone, position_x, position_y, position_z FROM character_homebind WHERE guid = :g', [':g' => $srcGuid]);
            $this->bulkInsertGuided($dstGuid, $dst, 'character_homebind', ['guid', 'map', 'zone', 'position_x', 'position_y', 'position_z'], $rows, $result, 'character_homebind');

            // character_honor_cp
            $rows = $this->fetchAll($src, 'SELECT victim_type, victim, honor, date, type, used FROM character_honor_cp WHERE guid = :g', [':g' => $srcGuid]);
            $this->bulkInsertGuided($dstGuid, $dst, 'character_honor_cp', ['guid', 'victim_type', 'victim', 'honor', 'date', 'type', 'used'], $rows, $result, 'character_honor_cp');

            // character_instance
            $rows = $this->fetchAll($src, 'SELECT instance, permanent FROM character_instance WHERE guid = :g', [':g' => $srcGuid]);
            $this->bulkInsertGuided($dstGuid, $dst, 'character_instance', ['guid', 'instance', 'permanent'], $rows, $result, 'character_instance');

            // character_queststatus
            $rows = $this->fetchAll($src, 'SELECT quest, status, rewarded, explored, timer, mobcount1, mobcount2, mobcount3, mobcount4, itemcount1, itemcount2, itemcount3, itemcount4 FROM character_queststatus WHERE guid = :g', [':g' => $srcGuid]);
            $this->bulkInsertGuided($dstGuid, $dst, 'character_queststatus', ['guid', 'quest', 'status', 'rewarded', 'explored', 'timer', 'mobcount1', 'mobcount2', 'mobcount3', 'mobcount4', 'itemcount1', 'itemcount2', 'itemcount3', 'itemcount4'], $rows, $result, 'character_queststatus');

            // character_reputation
            $rows = $this->fetchAll($src, 'SELECT faction, standing, flags FROM character_reputation WHERE guid = :g', [':g' => $srcGuid]);
            $this->bulkInsertGuided($dstGuid, $dst, 'character_reputation', ['guid', 'faction', 'standing', 'flags'], $rows, $result, 'character_reputation');

            // character_skills
            $rows = $this->fetchAll($src, 'SELECT skill, value, max FROM character_skills WHERE guid = :g', [':g' => $srcGuid]);
            $this->bulkInsertGuided($dstGuid, $dst, 'character_skills', ['guid', 'skill', 'value', 'max'], $rows, $result, 'character_skills');

            // character_spell
            $rows = $this->fetchAll($src, 'SELECT spell, active, disabled FROM character_spell WHERE guid = :g', [':g' => $srcGuid]);
            $this->bulkInsertGuided($dstGuid, $dst, 'character_spell', ['guid', 'spell', 'active', 'disabled'], $rows, $result, 'character_spell');

            // character_spell_cooldown
            $rows = $this->fetchAll($src, 'SELECT spell, item, time FROM character_spell_cooldown WHERE guid = :g', [':g' => $srcGuid]);
            $this->bulkInsertGuided($dstGuid, $dst, 'character_spell_cooldown', ['guid', 'spell', 'item', 'time'], $rows, $result, 'character_spell_cooldown');

            // character_stats
            $rows = $this->fetchAll($src, 'SELECT maxhealth, maxpower1, maxpower2, maxpower3, maxpower4, maxpower5, maxpower6, maxpower7, strength, agility, stamina, intellect, spirit, armor, resHoly, resFire, resNature, resFrost, resShadow, resArcane, blockPct, dodgePct, parryPct, critPct, rangedCritPct, attackPower, rangedAttackPower FROM character_stats WHERE guid = :g', [':g' => $srcGuid]);
            $this->bulkInsertGuided($dstGuid, $dst, 'character_stats', ['guid', 'maxhealth', 'maxpower1', 'maxpower2', 'maxpower3', 'maxpower4', 'maxpower5', 'maxpower6', 'maxpower7', 'strength', 'agility', 'stamina', 'intellect', 'spirit', 'armor', 'resHoly', 'resFire', 'resNature', 'resFrost', 'resShadow', 'resArcane', 'blockPct', 'dodgePct', 'parryPct', 'critPct', 'rangedCritPct', 'attackPower', 'rangedAttackPower'], $rows, $result, 'character_stats');

            // character_pet (map pet IDs; owner becomes dst guid)
            $rows = $this->fetchAll($src, 'SELECT id, entry, modelid, CreatedBySpell, PetType, level, exp, Reactstate, loyaltypoints, loyalty, trainpoint, name, renamed, slot, curhealth, curmana, curhappiness, savetime, resettalents_cost, resettalents_time, abdata, teachspelldata FROM character_pet WHERE owner = :g ORDER BY id', [':g' => $srcGuid]);
            $out = [];
            foreach ($rows as $r)
            {
                $sid = (int)$r['id'];
                if (!isset($petMap[$sid]))
                {
                    continue;
                }

                $out[] = [
                    'id' => $petMap[$sid],
                    'entry' => (int)$r['entry'],
                    'owner' => $dstGuid,
                    'modelid' => (int)$r['modelid'],
                    'CreatedBySpell' => (int)$r['CreatedBySpell'],
                    'PetType' => (int)$r['PetType'],
                    'level' => (int)$r['level'],
                    'exp' => (int)$r['exp'],
                    'Reactstate' => (int)$r['Reactstate'],
                    'loyaltypoints' => (int)$r['loyaltypoints'],
                    'loyalty' => (int)$r['loyalty'],
                    'trainpoint' => (int)$r['trainpoint'],
                    'name' => (string)$r['name'],
                    'renamed' => (int)$r['renamed'],
                    'slot' => (int)$r['slot'],
                    'curhealth' => (int)$r['curhealth'],
                    'curmana' => (int)$r['curmana'],
                    'curhappiness' => (int)$r['curhappiness'],
                    'savetime' => (int)$r['savetime'],
                    'resettalents_cost' => (int)$r['resettalents_cost'],
                    'resettalents_time' => (int)$r['resettalents_time'],
                    'abdata' => $r['abdata'],
                    'teachspelldata' => $r['teachspelldata'],
                ];
            }

            $this->bulkInsert($dst, 'character_pet', ['id', 'entry', 'owner', 'modelid', 'CreatedBySpell', 'PetType', 'level', 'exp', 'Reactstate', 'loyaltypoints', 'loyalty', 'trainpoint', 'name', 'renamed', 'slot', 'curhealth', 'curmana', 'curhappiness', 'savetime', 'resettalents_cost', 'resettalents_time', 'abdata', 'teachspelldata'], $out, $result, 'character_pet');

            // pet_spell
            $petIdsSrc = array_keys($petMap);
            sort($petIdsSrc);
            foreach (array_chunk($petIdsSrc, self::IN_CHUNK) as $chunk)
            {
                $in = $this->placeholders(count($chunk));
                $stmt = $src->prepare("SELECT guid, spell, active FROM pet_spell WHERE guid IN ($in)");
                $this->bindIntList($stmt, $chunk);
                $stmt->execute();
                $rows = $stmt->fetchAll() ?: [];
                $out = [];
                foreach ($rows as $r)
                {
                    $sg = (int)$r['guid'];
                    if (!isset($petMap[$sg]))
                    {
                        continue;
                    }

                    $out[] = [
                        'guid' => $petMap[$sg],
                        'spell' => (int)$r['spell'],
                        'active' => (int)$r['active'],
                    ];
                }

                $this->bulkInsert($dst, 'pet_spell', ['guid', 'spell', 'active'], $out, $result, 'pet_spell');
            }

            // pet_spell_cooldown
            foreach (array_chunk($petIdsSrc, self::IN_CHUNK) as $chunk)
            {
                $in = $this->placeholders(count($chunk));
                $stmt = $src->prepare("SELECT guid, spell, time FROM pet_spell_cooldown WHERE guid IN ($in)");
                $this->bindIntList($stmt, $chunk);
                $stmt->execute();
                $rows = $stmt->fetchAll() ?: [];
                $out = [];

                foreach ($rows as $r)
                {
                    $sg = (int)$r['guid'];
                    if (!isset($petMap[$sg]))
                    {
                        continue;
                    }

                    $out[] = [
                        'guid' => $petMap[$sg],
                        'spell' => (int)$r['spell'],
                        'time' => (int)$r['time'],
                    ];
                }

                $this->bulkInsert($dst, 'pet_spell_cooldown', ['guid', 'spell', 'time'], $out, $result, 'pet_spell_cooldown');
            }

            // pet_aura (map pet guid; map item_guid)
            foreach (array_chunk($petIdsSrc, self::IN_CHUNK) as $chunk)
            {
                $in = $this->placeholders(count($chunk));
                $stmt = $src->prepare("SELECT guid, caster_guid, item_guid, spell, stackcount, remaincharges, basepoints0, basepoints1, basepoints2, periodictime0, periodictime1, periodictime2, maxduration, remaintime, effIndexMask FROM pet_aura WHERE guid IN ($in)");
                $this->bindIntList($stmt, $chunk);
                $stmt->execute();
                $rows = $stmt->fetchAll() ?: [];
                $out = [];

                foreach ($rows as $r)
                {
                    $sg = (int)$r['guid'];
                    if (!isset($petMap[$sg]))
                    {
                        continue;
                    }

                    $ig = (int)$r['item_guid'];
                    $out[] = [
                        'guid' => $petMap[$sg],
                        'caster_guid' => (int)$r['caster_guid'],
                        'item_guid' => 0,
                        'spell' => (int)$r['spell'],
                        'stackcount' => (int)$r['stackcount'],
                        'remaincharges' => (int)$r['remaincharges'],
                        'basepoints0' => (int)$r['basepoints0'],
                        'basepoints1' => (int)$r['basepoints1'],
                        'basepoints2' => (int)$r['basepoints2'],
                        'periodictime0' => (int)$r['periodictime0'],
                        'periodictime1' => (int)$r['periodictime1'],
                        'periodictime2' => (int)$r['periodictime2'],
                        'maxduration' => (int)$r['maxduration'],
                        'remaintime' => (int)$r['remaintime'],
                        'effIndexMask' => (int)$r['effIndexMask'],
                    ];
                }

                $this->bulkInsert($dst, 'pet_aura', ['guid', 'caster_guid', 'item_guid', 'spell', 'stackcount', 'remaincharges', 'basepoints0', 'basepoints1', 'basepoints2', 'periodictime0', 'periodictime1', 'periodictime2', 'maxduration', 'remaintime', 'effIndexMask'], $out, $result, 'pet_aura');
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
        $row = $this->fetchRow(
            $src,
            'SELECT account, tut0, tut1, tut2, tut3, tut4, tut5, tut6, tut7 FROM character_tutorial WHERE account = :a',
            [':a' => $accountId]
        );

        if (!$row)
        {
            return;
        }

        $exists = (int) $this->fetchOne($dst, 'SELECT COUNT(*) FROM character_tutorial WHERE account = :a', [':a' => $accountId]);
        if ($exists > 0)
        {
            $sql = 'UPDATE character_tutorial SET
                        tut0 = (tut0 | :tut0),
                        tut1 = (tut1 | :tut1),
                        tut2 = (tut2 | :tut2),
                        tut3 = (tut3 | :tut3),
                        tut4 = (tut4 | :tut4),
                        tut5 = (tut5 | :tut5),
                        tut6 = (tut6 | :tut6),
                        tut7 = (tut7 | :tut7)
                    WHERE account = :a';
            $stmt = $dst->prepare($sql);
            $stmt->bindValue(':a', $accountId, PDO::PARAM_INT);

            for ($i = 0; $i <= 7; $i++)
            {
                $key = 'tut' . $i;
                $stmt->bindValue(':tut' . $i, (int) ($row[$key] ?? 0), PDO::PARAM_INT);
            }

            $stmt->execute();

            $result->counts['character_tutorial'] = ($result->counts['character_tutorial'] ?? 0) + $stmt->rowCount();
            return;
        }

        $this->insertOne($dst, 'character_tutorial', $this->tutorialColumns(), $row, $result, 'character_tutorial');
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
        $max = (int)$this->fetchOne($pdo, "SELECT COALESCE(MAX($col),0) FROM $table");
        return $max + 1;
    }

    private function sqlSourceItemGuids(): string
    {
        // IMPORTANT: do not reuse the same named placeholder (e.g., :g) multiple times.
        // MySQL/MariaDB PDO with native prepared statements will treat repeated names as
        // separate placeholders internally, causing HY093 unless each placeholder is bound.
        return "
            SELECT DISTINCT item AS src_item_guid
            FROM character_inventory
            WHERE guid = :g1 AND item <> 0
            UNION
            SELECT DISTINCT bag AS src_item_guid
            FROM character_inventory
            WHERE guid = :g2 AND bag <> 0
            UNION
            SELECT DISTINCT item_guid AS src_item_guid
            FROM character_gifts
            WHERE guid = :g3 AND item_guid <> 0
            ORDER BY src_item_guid
        ";
    }

    private function sqlCharactersSelect(): string
    {
        // Keep the column list aligned with character_migration_script.sql
        $cols = implode(', ', $this->charactersColumns());
        return "SELECT $cols FROM characters WHERE guid = :g";
    }

    /** @return array<int,string> */
    private function charactersColumns(): array
    {
        return [
            'guid', 'account', 'name', 'race', 'class', 'gender', 'level', 'xp', 'money', 'playerBytes', 'playerBytes2', 'playerFlags',
            'position_x', 'position_y', 'position_z', 'map', 'orientation', 'taximask', 'online', 'cinematic', 'totaltime', 'leveltime',
            'logout_time', 'is_logout_resting', 'rest_bonus', 'resettalents_cost', 'resettalents_time', 'trans_x', 'trans_y', 'trans_z',
            'trans_o', 'transguid', 'extra_flags', 'stable_slots', 'at_login', 'zone', 'death_expire_time', 'taxi_path',
            'honor_highest_rank', 'honor_standing', 'stored_honor_rating', 'stored_dishonorable_kills', 'stored_honorable_kills',
            'watchedFaction', 'drunk', 'health', 'power1', 'power2', 'power3', 'power4', 'power5', 'exploredZones', 'equipmentCache',
            'ammoId', 'actionBars', 'deleteInfos_Account', 'deleteInfos_Name', 'deleteDate', 'createdDate',
        ];
    }


    /** @return array<int,string> */
    private function tutorialColumns(): array
    {
        return ['account', 'tut0', 'tut1', 'tut2', 'tut3', 'tut4', 'tut5', 'tut6', 'tut7'];
    }

    /**
        * Insert a single associative row with explicit column order.
        * @param array<int,string> $columns
        * @param array<string,mixed> $row
        */
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
