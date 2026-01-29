<?php

declare(strict_types=1);

/** @var string $contentFile */
/** @var App\App $app */

$cfg = $app->config();
$appName = (string)($cfg['app']['name'] ?? 'Character Migration');
$flash = flash_get();

?>
<!doctype html>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title><?= htmlspecialchars($appName) ?></title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <link href="assets/css/app.css" rel="stylesheet">
    </head>
    <body>
        <div class="page-wrapper">
            <header class="site-header">
                <div class="site-header-inner">
                    <div class="site-logo"><?= htmlspecialchars($appName) ?></div>
                    <nav class="site-nav">
                        <a href="index.php">Home</a>
                    </nav>
                </div>
            </header>

            <main class="site-main">
                <?php if (!empty($flash)): ?>
                    <?php foreach ($flash as $f): ?>
                        <?php
                        $type = (string)($f['type'] ?? 'info');
                        $msg = (string)($f['message'] ?? '');
                        $cls = 'alert alert-' . $type;
                        ?>
                        <div class="<?= htmlspecialchars($cls) ?>">
                            <div><?= $msg ?></div>
                            <button class="close-btn" type="button" onclick="this.parentElement.remove()">×</button>
                        </div>
                    <?php endforeach; ?>
                <?php endif; ?>

                <?php require $contentFile; ?>
            </main>

            <footer class="site-footer">
                <div>Migration Tool</div>
            </footer>
        </div>
    </body>
</html>
