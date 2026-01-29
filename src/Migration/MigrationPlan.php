<?php

declare(strict_types=1);

namespace App\Migration;

final class MigrationPlan
{
    public string $mode; // 'single'|'all'
    public string $sourceProfile;
    public string $destProfile;

    /** @var int<0,max> */
    public int $srcGuid;
    /** @var int<0,max> */
    public int $dstGuidRequested;

    /** @var array<string,mixed> */
    public array $character = [];

    /** @var array<int,int> src_item_guid => dst_item_guid */
    public array $itemMap = [];

    /** Number of mapped item GUIDs that exist as rows in source item_instance. */
    public int $itemInstanceFound = 0;

    /** Number of mapped item GUIDs missing from source item_instance. */
    public int $itemInstanceMissing = 0;

    /** @var array<int,int> Sample of missing source item GUIDs (for UI display). */
    public array $missingItemInstanceSample = [];

    /** @var array<int,int> src_pet_id => dst_pet_id */
    public array $petMap = [];

    /** Whether this migration will force a rename on next login (destination name collision). */
    public bool $forceRename = false;

    /** @var array<int,array{src_guid:int,dst_guid:int,name:string,force_rename?:bool}> */
    public array $batch = [];

    public function __construct(
        string $mode,
        string $sourceProfile,
        string $destProfile,
        int $srcGuid,
        int $dstGuidRequested
    ) {
        $this->mode = $mode;
        $this->sourceProfile = $sourceProfile;
        $this->destProfile = $destProfile;
        $this->srcGuid = $srcGuid;
        $this->dstGuidRequested = $dstGuidRequested;
    }
}
