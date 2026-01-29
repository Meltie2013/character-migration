<?php

declare(strict_types=1);

require_once __DIR__ . '/bootstrap.php';

$profiles = $app->connections()->characterProfiles();
if (!$profiles)
{
    flash_set('danger', 'No character DB profiles configured. Edit config/config.php.');
}

$search = (string)($_GET['search'] ?? '');
$sourceProfile = (string)($_GET['source_profile'] ?? array_key_first($profiles));
$characters = [];

try
{
    if ($sourceProfile && isset($profiles[$sourceProfile]))
    {
        $limit = (int)($app->config()['ui']['character_dropdown_limit'] ?? 100);
        $characters = $service->listCharacters($sourceProfile, $search, $limit);
    }
}
catch (Throwable $e) 
{
    flash_set('danger', 'Failed to list characters: ' . $e->getMessage());
}

render('pages/index_page.php', [
    'profiles' => $profiles,
    'characters' => $characters,
    'search' => $search,
    'sourceProfile' => $sourceProfile,
]);
