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

$req = $_SESSION['cm_request'] ?? null;
if (!is_array($req) || !isset($req['mode'],$req['source_profile'],$req['dest_profile']))
{
    flash_set('warning', 'No pending migration request found. Start from the first page.');
    header('Location: index.php');
    exit;
}

try
{

    if ((string)$req['source_profile'] === (string)$req['dest_profile'])
    {
        throw new \App\Migration\MigrationException('From and To databases must be different.');
    }
    if ($req['mode'] === 'all')
    {
        $result = $service->executeBatch((string)$req['source_profile'], (string)$req['dest_profile']);
        render('pages/final_page.php', [
            'mode' => 'all',
            'result' => $result,
        ]);

        exit;
    }

    $srcGuid = (int)($req['src_guid'] ?? 0);
    $dstGuid = (int)($req['dst_guid'] ?? 0);
    $plan = $service->buildSinglePlan((string)$req['source_profile'], (string)$req['dest_profile'], $srcGuid, $dstGuid);
    $result = $service->executeSingle($plan);

    render('pages/final_page.php', [
        'mode' => 'single',
        'result' => $result,
    ]);
}
catch (Throwable $e) 
{
    flash_set('danger', $e->getMessage());
    header('Location: index.php');
    exit;
}
