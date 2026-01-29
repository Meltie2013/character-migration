<?php
declare(strict_types=1);

namespace App;

use App\Db\ConnectionManager;
use App\Security\Csrf;

final class App
{
    /** @var array<string,mixed> */
    private array $config;

    private ConnectionManager $connections;
    private Csrf $csrf;

    public function __construct()
    {
        $this->config = require dirname(__DIR__) . '/config/config.php';
        date_default_timezone_set((string)($this->config['app']['timezone'] ?? 'UTC'));

        $this->initSession();
        $this->initErrorHandling();

        $this->connections = new ConnectionManager($this->config);
        $this->csrf = new Csrf();
    }

    /** @return array<string,mixed> */
    public function config(): array
    {
        return $this->config;
    }

    public function connections(): ConnectionManager
    {
        return $this->connections;
    }

    public function csrf(): Csrf
    {
        return $this->csrf;
    }

    private function initSession(): void
    {
        if (session_status() === PHP_SESSION_ACTIVE) {
            return;
        }

        ini_set('session.use_strict_mode', '1');
        ini_set('session.cookie_httponly', '1');
        // If you serve over HTTPS, set to 1.
        ini_set('session.cookie_secure', '0');
        ini_set('session.cookie_samesite', 'Lax');

        session_start();
    }

    private function initErrorHandling(): void
    {
        $debug = (bool)($this->config['app']['debug'] ?? false);
        $logFile = (string)($this->config['app']['log_file'] ?? '');

        set_error_handler(function (int $severity, string $message, string $file, int $line) use ($debug, $logFile): bool {
            if (!(error_reporting() & $severity)) {
                return false;
            }
            $ex = new \ErrorException($message, 0, $severity, $file, $line);
            $this->logException($ex, $logFile);
            if ($debug) {
                throw $ex;
            }
            http_response_code(500);
            echo 'An internal error occurred.';
            return true;
        });

        set_exception_handler(function (\Throwable $ex) use ($debug, $logFile): void {
            $this->logException($ex, $logFile);
            http_response_code(500);
            if ($debug) {
                echo '<pre>' . htmlspecialchars((string)$ex) . '</pre>';
                return;
            }
            echo 'An internal error occurred.';
        });
    }

    private function logException(\Throwable $ex, string $logFile): void
    {
        if ($logFile === '') {
            return;
        }
        $dir = dirname($logFile);
        if (!is_dir($dir)) {
            @mkdir($dir, 0750, true);
        }
        $line = sprintf(
            "[%s] %s: %s in %s:%d\nStack: %s\n\n",
            date('c'),
            get_class($ex),
            $ex->getMessage(),
            $ex->getFile(),
            $ex->getLine(),
            $ex->getTraceAsString()
        );
        @file_put_contents($logFile, $line, FILE_APPEND);
    }
}
