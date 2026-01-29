<?php

declare(strict_types=1);

use App\Support\CharacterEnums;

/** @var App\Migration\MigrationPlan $plan */

$csrfToken = $csrf->token();

$ui = $app->config()['ui'] ?? [];
$showGuidSamples = (bool)($ui['show_guid_samples'] ?? false);
$guidSampleLimit = (int)($ui['guid_sample_limit'] ?? 25);


$profiles = $app->connections()->characterProfiles();
$fromDbName = (string)($profiles[$plan->sourceProfile]['realm_name'] ?? $plan->sourceProfile);
$toDbName = (string)($profiles[$plan->destProfile]['realm_name'] ?? $plan->destProfile);

/**
    * Render a two-column table for a group of fields, showing only fields that exist in $row.
    *
    * @param array<string,mixed> $row
    * @param array<string,string> $fields key => label
    */
$renderFieldTable = static function (array $row, array $fields): void {
    $shown = 0;

    echo '<div class="table-responsive">';
    echo '<table class="table table-sm align-middle mb-0">';
    echo '<tbody>';

    foreach ($fields as $key => $label)
    {
        if (!array_key_exists($key, $row))
        {
            continue;
        }

        $val = $row[$key];

        if ($val === null)
        {
            $valStr = '';
        }
        elseif (is_bool($val))
        {
            $valStr = $val ? '1' : '0';
        }
        elseif (is_scalar($val))
        {
            $valStr = (string)$val;
        }
        else
        {
            $valStr = (string)json_encode($val);
        }

        echo '<tr>';
        echo '<td class="text-muted small" style="width: 35%;">' . htmlspecialchars($label) . '</td>';
        echo '<td class="code">' . htmlspecialchars($valStr) . '</td>';
        echo '</tr>';

        $shown++;
    }

    if ($shown === 0)
    {
        echo '<tr><td class="text-muted">No fields available.</td><td></td></tr>';
    }

    echo '</tbody>';
    echo '</table>';
    echo '</div>';
};
?>

<div class="d-flex align-items-center justify-content-between flex-wrap gap-2 mb-3">
    <div>
        <h1 class="h4 mb-1">Preview</h1>
        <div class="text-muted">Review the source character snapshot and planned identifiers before execution.</div>
    </div>
    <div class="d-flex gap-2">
        <a class="btn btn-outline-secondary" href="index.php">Back</a>
        <form method="post" action="migrate.php" class="m-0">
            <input type="hidden" name="csrf" value="<?= htmlspecialchars($csrfToken) ?>">
            <button type="submit" class="btn btn-primary">Execute Migration</button>
        </form>
    </div>
</div>

<?php if ($plan->mode === 'single'): ?>
    <?php $c = $plan->character; ?>

    <?php
    $cDisplay = $c;
    $cDisplay['race'] = CharacterEnums::raceName((int)($c['race'] ?? 0));
    $cDisplay['class'] = CharacterEnums::className((int)($c['class'] ?? 0));
    $cDisplay['gender'] = CharacterEnums::genderName((int)($c['gender'] ?? 0));
    ?>

    <?php
    $identity = [
        'guid' => 'GUID (source)',
        'dst_guid' => 'GUID (destination)',
        'account' => 'Account ID',
        'name' => 'Name (source)',
        'race' => 'Race',
        'class' => 'Class',
        'gender' => 'Gender',
    ];

    $progression = [
        'level' => 'Level',
        'money' => 'Money',
        'xp' => 'XP',
    ];
    ?>

    <div class="row g-4">
        <div class="col-12">
            <div class="card shadow-sm">
                <div class="card-body">
                    <div class="d-flex justify-content-between align-items-start flex-wrap gap-2">
                        <div>
                            <h2 class="h6 mb-1">Source Character Snapshot</h2>
                            <div class="text-muted small">
                                From: <span class="badge badge-soft"><?= htmlspecialchars($fromDbName) ?></span>
                                &nbsp;&nbsp;To: <span class="badge badge-soft"><?= htmlspecialchars($toDbName) ?></span>
                            </div>
                        </div>
                        <div class="text-muted small">
                            Destination GUIDs are computed from current destination max values.
                        </div>
                    </div>

                    <div class="row g-4 mt-1">
                        <div class="col-lg-6">
                            <div class="border rounded-3 p-3">
                                <div class="text-muted small mb-2">Identity</div>
                                <?php $renderFieldTable($cDisplay, $identity); ?>
                            </div>
                        </div>

                        <div class="col-lg-6">
                            <div class="border rounded-3 p-3">
                                <div class="text-muted small mb-2">Progression</div>
                                <?php $renderFieldTable($c, $progression); ?>
                            </div>
                        </div>

                    <?php if ((bool)($plan->character['force_rename'] ?? $plan->forceRename ?? false)): ?>
                        <div class="col-12">
                            <div class="alert alert-warning mb-0">
                                <div class="fw-semibold mb-1">Rename will be forced on next login</div>
                            </div>
                        </div>
                    <?php endif; ?>
                    <?php if ((bool)($plan->character['tutorial_src_exists'] ?? false)): ?>
                        <div class="col-12">
                            <div class="alert alert-info mb-0">
                                <div class="fw-semibold mb-1">Tutorial completion data will be transferred</div>
                            </div>
                        </div>
                    <?php endif; ?>

                    </div>
                </div>
            </div>
        </div>

        <div class="col-12">

            <div class="card shadow-sm">
                <div class="card-body">
                    <h2 class="h6">Planned GUID Mapping (Character-owned)</h2>

                    <div class="row g-3">
                        <div class="col-md-4">
                            <div class="text-muted small">Item GUIDs referenced</div>
                            <div class="h5 mb-0"><?= count($plan->itemMap) ?></div>
                        </div>
                        <div class="col-md-4">
                            <div class="text-muted small">Item records found</div>
                            <div class="h5 mb-0"><?= (int)$plan->itemInstanceFound ?></div>
                            <?php if ((int)$plan->itemInstanceMissing > 0): ?>
                                <div class="text-warning small mt-1"><?= (int)$plan->itemInstanceMissing ?> referenced item GUID(s) are missing item records in source.</div>
                            <?php endif; ?>
                        </div>
                        <div class="col-md-4">
                            <div class="text-muted small">Pets mapped</div>
                            <div class="h5 mb-0"><?= count($plan->petMap) ?></div>
                        </div>
                    </div>



                    <?php if ($showGuidSamples && $plan->itemMap): ?>
                        <div class="mt-3">
                            <div class="text-muted small mb-2">Item GUID translations (sample)</div>
                            <div class="table-responsive">
                                <table class="table table-sm align-middle">
                                    <thead>
                                        <tr>
                                            <th>Source Item GUID</th>
                                            <th>Destination Item GUID</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <?php $i = 0;
                                        foreach ($plan->itemMap as $src => $dst): $i++;
                                            if ($i > $guidSampleLimit) break; ?>
                                            <tr>
                                                <td class="code"><?= (int)$src ?></td>
                                                <td class="code"><?= (int)$dst ?></td>
                                            </tr>
                                        <?php endforeach; ?>
                                    </tbody>
                                </table>
                            </div>
                            <?php if (count($plan->itemMap) > $guidSampleLimit): ?>
                                <div class="text-muted small">Showing first <?= (int)$guidSampleLimit ?> rows.</div>
                            <?php endif; ?>
                        </div>
                    <?php endif; ?>

                    <?php if ($showGuidSamples && $plan->petMap): ?>
                        <div class="mt-3">
                            <div class="text-muted small mb-2">Pet ID translations (sample)</div>
                            <div class="table-responsive">
                                <table class="table table-sm align-middle">
                                    <thead>
                                        <tr>
                                            <th>Source Pet ID</th>
                                            <th>Destination Pet ID</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <?php $i = 0;
                                        foreach ($plan->petMap as $src => $dst): $i++;
                                            if ($i > $guidSampleLimit) break; ?>
                                            <tr>
                                                <td class="code"><?= (int)$src ?></td>
                                                <td class="code"><?= (int)$dst ?></td>
                                            </tr>
                                        <?php endforeach; ?>
                                    </tbody>
                                </table>
                            </div>
                            <?php if (count($plan->petMap) > $guidSampleLimit): ?>
                                <div class="text-muted small">Showing first <?= (int)$guidSampleLimit ?> rows.</div>
                            <?php endif; ?>
                        </div>
                    <?php endif; ?>
                </div>
            </div>
        </div>
    </div>
<?php else: ?>
    <div class="card shadow-sm">
        <div class="card-body">
            <div class="d-flex justify-content-between align-items-start flex-wrap gap-2">
                <div>
                    <h2 class="h6 mb-1">Batch Migration Preview</h2>
                    <div class="text-muted small">
                        From: <span class="badge badge-soft"><?= htmlspecialchars($fromDbName) ?></span>
                        &nbsp;&nbsp;To: <span class="badge badge-soft"><?= htmlspecialchars($toDbName) ?></span>
                    </div>
                </div>
                <div class="text-muted small">
                    This list shows the next predicted destination GUID per source GUID, based on current destination max values.
                </div>
            </div>

            <div class="table-responsive mt-3">
                <table class="table table-sm align-middle">
                    <thead>
                        <tr>
                            <th>Source GUID</th>
                            <th>Predicted Destination GUID</th>
                            <th>Name</th>
                            <th>Rename</th>
                        </tr>
                    </thead>
                    <tbody>
                        <?php $shown = 0;
                        foreach ($plan->batch as $r): $shown++;
                            if ($shown > 200) break; ?>
                            <tr>
                                <td class="code"><?= (int)$r['src_guid'] ?></td>
                                <td class="code"><?= (int)$r['dst_guid'] ?></td>
                                <td><?= htmlspecialchars($r['name']) ?></td>
                                <td>
                                    <?php if (!empty($r['force_rename'])): ?>
                                        <span class="badge text-bg-warning">Forced</span>
                                    <?php else: ?>
                                        <span class="text-muted">—</span>
                                    <?php endif; ?>
                                </td>
                            </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>

            <?php if (count($plan->batch) > 200): ?>
                <div class="text-muted small">Showing first 200 rows.</div>
            <?php endif; ?>
        </div>
    </div>
<?php endif; ?>
