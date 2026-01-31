<?php

declare(strict_types=1);

require_once __DIR__ . '/bootstrap.php';

if ($_SERVER['REQUEST_METHOD'] !== 'POST')
{
    header('Location: index.php');
    exit;
}

if (!$app->csrf()->validate($_POST['csrf'] ?? null))
{
    abort(403, 'Invalid CSRF token.');
}

$mode = (string)($_POST['mode'] ?? 'single');
$sourceProfile = (string)($_POST['source_profile'] ?? '');
$destProfile = (string)($_POST['dest_profile'] ?? '');

if ($sourceProfile === $destProfile)
{
    throw new \App\Migration\MigrationException('From and To databases must be different.');
}

$srcGuid = (int)($_POST['src_guid'] ?? 0);
$dstGuid = (int)($_POST['dst_guid'] ?? 0);

try
{
    if ($mode === 'all')
    {
        $plan = $service->buildBatchPlan($sourceProfile, $destProfile);
        $_SESSION['cm_request'] = [
            'mode' => 'all',
            'source_profile' => $sourceProfile,
            'dest_profile' => $destProfile,
        ];
    }
    else
    {
        if ($srcGuid <= 0)
        {
            throw new \App\Migration\MigrationException('Please select a character GUID for single-character mode.');
        }

        $plan = $service->buildSinglePlan($sourceProfile, $destProfile, $srcGuid, $dstGuid);
        $_SESSION['cm_request'] = [
            'mode' => 'single',
            'source_profile' => $sourceProfile,
            'dest_profile' => $destProfile,
            'src_guid' => $srcGuid,
            'dst_guid' => $dstGuid,
        ];
    }

    render('pages/preview_page.php', [
        'plan' => $plan,
    ]);
}
catch (Throwable $e)
{
    flash_set('danger', $e->getMessage());
    header('Location: index.php');
    exit;
}
