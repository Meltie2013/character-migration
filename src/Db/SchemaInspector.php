<?php

declare(strict_types=1);

namespace App\Db;

use PDO;
use PDOException;

final class SchemaInspector
{
    /** @var array<string, array<string, array<int,string>>> */
    private array $columnsCache = [];

    /** @var array<string, array<string, bool>> */
    private array $tableExistsCache = [];

    /**
     * @return array<int,string> Columns in table order.
     */
    public function columns(PDO $pdo, string $table): array
    {
        $dbKey = $this->dbCacheKey($pdo);

        if (isset($this->columnsCache[$dbKey][$table]))
        {
            return $this->columnsCache[$dbKey][$table];
        }

        if (!isset($this->tableExistsCache[$dbKey][$table]))
        {
            $this->tableExistsCache[$dbKey][$table] = $this->tableExists($pdo, $table);
        }

        if (!$this->tableExistsCache[$dbKey][$table])
        {
            $this->columnsCache[$dbKey][$table] = [];
            return [];
        }

        $safe = $this->quoteIdent($table);

        try
        {
            $stmt = $pdo->query('SHOW COLUMNS FROM ' . $safe);
            $rows = $stmt ? ($stmt->fetchAll() ?: []) : [];
        }
        catch (PDOException $e)
        {
            // If we cannot inspect, treat as missing.
            $this->tableExistsCache[$dbKey][$table] = false;
            $this->columnsCache[$dbKey][$table] = [];
            return [];
        }

        $cols = [];
        foreach ($rows as $r)
        {
            $f = (string)($r['Field'] ?? '');
            if ($f !== '')
            {
                $cols[] = $f;
            }
        }

        $this->columnsCache[$dbKey][$table] = $cols;
        return $cols;
    }

    public function tableExists(PDO $pdo, string $table): bool
    {
        $dbKey = $this->dbCacheKey($pdo);
        if (isset($this->tableExistsCache[$dbKey][$table]))
        {
            return $this->tableExistsCache[$dbKey][$table];
        }

        $safe = $this->quoteIdent($table);

        try
        {
            $pdo->query('SHOW TABLES LIKE ' . $pdo->quote($table));
            // Not reliable to parse in a portable way; instead attempt SHOW COLUMNS.
            $stmt = $pdo->query('SHOW COLUMNS FROM ' . $safe);
            $ok = (bool)$stmt;
        }
        catch (PDOException $e)
        {
            $ok = false;
        }

        $this->tableExistsCache[$dbKey][$table] = $ok;
        return $ok;
    }

    public function columnExists(PDO $pdo, string $table, string $column): bool
    {
        $cols = $this->columns($pdo, $table);
        foreach ($cols as $c)
        {
            if ($c === $column)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * @param array<int,string> $required
     * @return array<int,string> Columns present in BOTH schemas, in destination table order.
     */
    public function commonColumns(PDO $src, PDO $dst, string $table, array $required = []): array
    {
        $srcCols = $this->columns($src, $table);
        $dstCols = $this->columns($dst, $table);

        if (!$srcCols || !$dstCols)
        {
            return [];
        }

        $srcSet = [];
        foreach ($srcCols as $c)
        {
            $srcSet[$c] = true;
        }

        $common = [];
        foreach ($dstCols as $c)
        {
            if (isset($srcSet[$c]))
            {
                $common[] = $c;
            }
        }

        foreach ($required as $req)
        {
            $found = false;
            foreach ($common as $c)
            {
                if ($c === $req)
                {
                    $found = true;
                    break;
                }
            }

            if (!$found)
            {
                return [];
            }
        }

        return $common;
    }

    /**
     * @param array<int,string> $columns
     */
    public function selectList(array $columns): string
    {
        $parts = [];
        foreach ($columns as $c)
        {
            $parts[] = $this->quoteIdent($c);
        }
        return implode(', ', $parts);
    }

    private function dbCacheKey(PDO $pdo): string
    {
        // Use the current database as part of the cache key.
        try
        {
            $db = (string)($pdo->query('SELECT DATABASE()')->fetchColumn() ?: '');
        }
        catch (PDOException $e)
        {
            $db = '';
        }

        return spl_object_hash($pdo) . '|' . $db;
    }

    private function quoteIdent(string $name): string
    {
        // Only allow simple identifiers (table/column names in this app are static).
        if (!preg_match('/^[A-Za-z0-9_]+$/', $name))
        {
            throw new \InvalidArgumentException('Invalid identifier: ' . $name);
        }

        return '`' . $name . '`';
    }

    /**
     * Detect the likely core/expansion structure for a MaNGOS character database.
     *
     * This is heuristic-based on the presence of well-known tables/columns.
     * The migration engine itself remains schema-driven and does not depend on
     * this label; it is used only for UI visibility/debugging.
     *
     * @return array{code:string,label:string,summary:string}
     */
    public function detectCharacterDbStructure(PDO $pdo): array
    {
        if (!$this->tableExists($pdo, 'characters'))
        {
            return [
                'code' => 'unknown',
                'label' => 'Unknown',
                'summary' => 'Missing required table: characters',
            ];
        }

        // Common indicators.
        $hasArenaTeam = $this->tableExists($pdo, 'arena_team');
        $hasArenaTeamMember = $this->tableExists($pdo, 'arena_team_member');
        $hasArenaPoints = $this->columnExists($pdo, 'characters', 'arenaPoints');
        $hasTotalHonor = $this->columnExists($pdo, 'characters', 'totalHonorPoints');

        $charCols = $this->columns($pdo, 'characters');
        $charCount = count($charCols);
        $itemCount = $this->tableExists($pdo, 'item_instance') ? count($this->columns($pdo, 'item_instance')) : 0;
        $invCount = $this->tableExists($pdo, 'character_inventory') ? count($this->columns($pdo, 'character_inventory')) : 0;

        // Determine label.
        // TBC introduces arena tables and arenaPoints in many MaNGOS schema variants.
        if ($hasArenaTeam || $hasArenaTeamMember || $hasArenaPoints)
        {
            $code = 'mangos_one';
            $label = 'Mangos One (TBC)';
        }
        else
        {
            $code = 'mangos_zero';
            $label = 'Mangos Zero (Vanilla)';
        }

        $summary = sprintf(
            'characters=%d cols; item_instance=%d cols; character_inventory=%d cols; arena_team=%s; arenaPoints=%s; totalHonorPoints=%s',
            $charCount,
            $itemCount,
            $invCount,
            $hasArenaTeam ? 'yes' : 'no',
            $hasArenaPoints ? 'yes' : 'no',
            $hasTotalHonor ? 'yes' : 'no'
        );

        return [
            'code' => $code,
            'label' => $label,
            'summary' => $summary,
        ];
    }
}
