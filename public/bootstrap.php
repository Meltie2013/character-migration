<?php

declare(strict_types=1);

// Manual includes (no Composer required)
require_once dirname(__DIR__) . '/src/App.php';
require_once dirname(__DIR__) . '/src/Support/CharacterEnums.php';
require_once dirname(__DIR__) . '/src/Db/PdoFactory.php';
require_once dirname(__DIR__) . '/src/Db/ConnectionManager.php';
require_once dirname(__DIR__) . '/src/Security/Csrf.php';
require_once dirname(__DIR__) . '/src/Migration/MigrationException.php';
require_once dirname(__DIR__) . '/src/Migration/MigrationPlan.php';
require_once dirname(__DIR__) . '/src/Migration/MigrationResult.php';
require_once dirname(__DIR__) . '/src/Migration/MigrationService.php';

use App\App;
use App\Migration\MigrationService;

$app = new App();
$ui = $app->config()['ui'] ?? [];
$sampleLimit = (int)($ui['guid_sample_limit'] ?? 25);
$service = new MigrationService($app->connections(), $sampleLimit);

/** @return never */
function abort(int $code, string $message): void
{
    http_response_code($code);
    echo htmlspecialchars($message, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
    exit;
}

/**
    * @param string $template relative to /templates
    * @param array<string,mixed> $params
    */
function render(string $template, array $params = []): void
{
    $templatePath = dirname(__DIR__) . '/templates/' . ltrim($template, '/');
    if (!is_file($templatePath))
    {
        abort(500, 'Template not found: ' . $template);
    }

    // Provide $app, $service, and $csrf to all templates
    global $app, $service;
    $csrf = $app->csrf();

    extract($params, EXTR_SKIP);
    $contentFile = $templatePath;
    require dirname(__DIR__) . '/templates/layout.php';
}

function flash_set(string $type, string $message): void
{
    $_SESSION['flash'] = ['type' => $type, 'message' => $message];
}

/** @return array{type:string,message:string}|null */
function flash_get(): ?array
{
    if (!isset($_SESSION['flash']) || !is_array($_SESSION['flash']))
    {
        return null;
    }

    $f = $_SESSION['flash'];
    unset($_SESSION['flash']);
    if (!isset($f['type'], $f['message']))
    {
        return null;
    }

    return ['type' => (string)$f['type'], 'message' => (string)$f['message']];
}
