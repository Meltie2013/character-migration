<?php

declare(strict_types=1);

use App\Support\CharacterEnums;

/** @var string $mode */
/** @var App\Migration\MigrationResult $result */

?>

<div class="d-flex align-items-center justify-content-between flex-wrap gap-2 mb-3">
    <div>
    <h1 class="h4 mb-1">Results</h1>
    <div class="text-muted">Execution completed. Review inserted counts and final records below.</div>
    </div>
    <div class="d-flex gap-2">
    <a class="btn btn-outline-secondary" href="index.php">Start another migration</a>
    </div>
</div>

<div class="row g-4">
    <div class="col-lg-7">
    <div class="card shadow-sm">
        <div class="card-body">
        <h2 class="h6">Migration Report</h2>
        <div class="table-responsive">
            <table class="table table-sm align-middle">
            <thead>
                <tr>
                <th>Old</th>
                <th>New</th>
                <th>Name</th>
                <th>Status</th>
                <th>Message</th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($result->rows as $r): ?>
                <tr>
                    <td class="code"><?= (int) $r['src_guid'] ?></td>
                    <td class="code"><?= (int) $r['dst_guid'] ?></td>
                    <td><?= htmlspecialchars((string) $r['name']) ?></td>
                    <td>
                    <?php if ($r['status'] === 'OK'): ?>
                        <span class="badge text-bg-success">OK</span>
                    <?php else: ?>
                        <span class="badge text-bg-danger">FAILED</span>
                    <?php endif; ?>
                    </td>
                    <td class="text-muted small"><?= htmlspecialchars((string) $r['message']) ?></td>
                </tr>
                <?php endforeach; ?>
            </tbody>
            </table>
        </div>
        </div>
    </div>

    <?php if ($mode === 'single' && $result->finalCharacter): ?>
        <div class="card shadow-sm mt-4">
        <div class="card-body">
            <h2 class="h6">Destination Character</h2>
            <div class="row g-3">
            <div class="col-md-6">
                <div class="text-muted small">GUID</div>
                <div class="code"><?= (int) ($result->finalCharacter['guid'] ?? 0) ?></div>
            </div>
            <div class="col-md-6">
                <div class="text-muted small">Name</div>
                <div><strong><?= htmlspecialchars((string) ($result->finalCharacter['name'] ?? '')) ?></strong></div>
            </div>
            <div class="col-md-4">
                <div class="text-muted small">Level</div>
                <div class="code"><?= (int) ($result->finalCharacter['level'] ?? 0) ?></div>
            </div>
            <div class="col-md-4">
                <div class="text-muted small">Race / Class / Gender</div>
                <div class="code">
                    <?= htmlspecialchars(CharacterEnums::raceName((int) ($result->finalCharacter['race'] ?? 0))) ?> /
                    <?= htmlspecialchars(CharacterEnums::className((int) ($result->finalCharacter['class'] ?? 0))) ?> /
                    <?= htmlspecialchars(CharacterEnums::genderName((int) ($result->finalCharacter['gender'] ?? 0))) ?>
                </div>
            </div>
            <div class="col-md-4">
                <div class="text-muted small">Money</div>
                <div class="code"><?= (int) ($result->finalCharacter['money'] ?? 0) ?></div>
            </div>
            </div>
        </div>
        </div>
    <?php endif; ?>
    </div>

    <div class="col-lg-5">
        <div class="card shadow-sm">
            <div class="card-body">
            <h2 class="h6">Inserted Row Counts</h2>
            <?php if (!$result->counts): ?>
                <div class="text-muted">No inserts were recorded.</div>
            <?php else: ?>
                <div class="table-responsive">
                <table class="table table-sm align-middle">
                    <thead>
                    <tr>
                        <th>Table</th>
                        <th class="text-end">Rows</th>
                    </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($result->counts as $k => $v): ?>
                        <tr>
                        <td class="code"><?= htmlspecialchars($k) ?></td>
                        <td class="text-end code"><?= (int) $v ?></td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
                </div>
            <?php endif; ?>

            <?php if (!empty($result->warnings)): ?>
                <div class="alert alert-warning mt-3 mb-0">
                <div class="fw-semibold mb-1">Warnings</div>
                <ul class="mb-0">
                    <?php foreach ($result->warnings as $w): ?>
                    <li><?= htmlspecialchars((string) $w) ?></li>
                    <?php endforeach; ?>
                </ul>
                </div>
            <?php endif; ?>
            </div>
        </div>
    </div>
</div>
