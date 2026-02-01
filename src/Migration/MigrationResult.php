<?php
declare(strict_types=1);

namespace App\Migration;

final class MigrationResult
{
    /** @var array<int, array{src_guid:int, dst_guid:int, name:string, status:string, message:string}> */
    public array $rows = [];

    /** @var array<string, int> */
    public array $counts = [];

    /**
        * Non-fatal findings discovered during execution (e.g., dangling item references).
        *
        * @var array<int, string>
        */
    public array $warnings = [];

    /** @var array<string, mixed> */
    public array $finalCharacter = [];

    /** Detected schema label for the source character database (UI-only). */
    public string $sourceSchemaLabel = '';

    /** Detected schema label for the destination character database (UI-only). */
    public string $destSchemaLabel = '';
}
