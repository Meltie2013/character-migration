<?php

declare(strict_types=1);

/** @var array<string,array<string,mixed>> $profiles */
/** @var array<int,array<string,mixed>> $characters */
/** @var string $search */
/** @var string $sourceProfile */

$csrfToken = $csrf->token();
$profileNames = array_keys($profiles);
$defaultSource = $sourceProfile ?: ($profileNames[0] ?? '');
$defaultDest = ($profileNames[1] ?? $defaultSource);
?>

<div class="row g-4">
    <div class="col-lg-8">
    <div class="card shadow-sm">
        <div class="card-body">
        <h1 class="h4 mb-3">Start a Migration</h1>

        <form class="row g-3" method="post" action="preview.php">
            <input type="hidden" name="csrf" value="<?= htmlspecialchars($csrfToken) ?>">

            <div class="col-md-6">
            <label class="form-label">From</label>
            <select class="form-select" name="source_profile" required onchange="window.location='index.php?source_profile='+encodeURIComponent(this.value)+'&search='+encodeURIComponent(document.getElementById('search').value)">
                <?php foreach ($profileNames as $name): ?>
                <option value="<?= htmlspecialchars($name) ?>" <?= $name === $defaultSource ? 'selected' : '' ?>>
                    <?= htmlspecialchars((string)$profiles[$name]['realm_name']) ?>
                </option>
                <?php endforeach; ?>
            </select>
            <div class="form-text">Select the character schema you are migrating from.</div>
            </div>

            <div class="col-md-6">
            <label class="form-label">To</label>
            <select class="form-select" name="dest_profile" required>
                <?php foreach ($profileNames as $name): ?>
                <option value="<?= htmlspecialchars($name) ?>" <?= $name === $defaultDest ? 'selected' : '' ?>>
                    <?= htmlspecialchars((string)$profiles[$name]['realm_name']) ?>
                </option>
                <?php endforeach; ?>
            </select>
            <div class="form-text">Select the character schema you are migrating to.</div>
            </div>

            <div class="col-12">
            <label class="form-label">Mode</label>
            <div class="d-flex flex-wrap gap-3">
                <div class="form-check">
                <input class="form-check-input" type="radio" name="mode" id="mode_single" value="single" checked>
                <label class="form-check-label" for="mode_single">Single character</label>
                </div>
                <div class="form-check">
                <input class="form-check-input" type="radio" name="mode" id="mode_all" value="all">
                <label class="form-check-label" for="mode_all">All characters</label>
                </div>
            </div>
            </div>

            <div class="col-12">
            <div class="card card-subtle">
                <div class="card-body">
                <div class="d-flex align-items-end gap-2 flex-wrap">
                    <div class="flex-grow-1">
                    <label class="form-label">Find character (optional)</label>
                    <input class="form-control" id="search" value="<?= htmlspecialchars($search) ?>" placeholder="Search by name or GUID">
                    <div class="form-text">This search only refreshes the dropdown list below.</div>
                    </div>
                    <div>
                    <button type="button" class="btn btn-outline-secondary"
                        onclick="window.location='index.php?source_profile='+encodeURIComponent(document.querySelector('select[name=source_profile]').value)+'&search='+encodeURIComponent(document.getElementById('search').value)">Search</button>
                    </div>
                </div>

                <div class="mt-3">
                    <label class="form-label">Select character to migrate (single mode)</label>
                    <select class="form-select" name="src_guid">
                    <option value="0">-- Choose a character (or use All Characters) --</option>
                    <?php foreach ($characters as $c): ?>
                        <option value="<?= (int)$c['guid'] ?>">
                        <?= (int)$c['guid'] ?> — <?= htmlspecialchars((string)$c['name']) ?> (Lvl <?= (int)$c['level'] ?>)
                        </option>
                    <?php endforeach; ?>
                    </select>
                </div>

                <div class="mt-3">
                    <label class="form-label">Destination GUID (optional, single mode only)</label>
                    <input class="form-control" type="number" min="0" name="dst_guid" value="0">
                    <div class="form-text">
                    Set to <span class="code">0</span> (recommended) to auto-assign the next available GUID.
                    In all-characters mode, this will be forced to 0.
                    </div>
                </div>
                </div>
            </div>
            </div>

            <div class="col-12 d-flex justify-content-end gap-2">
            <button type="submit" class="btn btn-primary">Continue to Preview</button>
            </div>
        </form>
        </div>
    </div>
    </div>

    <div class="col-lg-4">
    <div class="card shadow-sm">
        <div class="card-body">
        <h2 class="h6">Operational Notes</h2>
        <ul class="small text-muted mb-0">
            <li>Each character runs in its own destination transaction.</li>
            <li>Foreign key checks are disabled during insertion and restored afterward.</li>
            <li>Names are copied as-is (trimmed to 12 characters). No rename/suffix logic.</li>
            <li>Only character-owned tables are migrated (no guilds, mail, auctions).</li>
        </ul>
        </div>
    </div>
    </div>
</div>
