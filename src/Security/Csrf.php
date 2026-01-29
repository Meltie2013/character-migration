<?php
declare(strict_types=1);

namespace App\Security;

final class Csrf
{
    private const SESSION_KEY = 'csrf_token_v1';

    public function token(): string
    {
        if (isset($_SESSION[self::SESSION_KEY]) && is_string($_SESSION[self::SESSION_KEY])) {
            return $_SESSION[self::SESSION_KEY];
        }
        $token = bin2hex(random_bytes(32));
        $_SESSION[self::SESSION_KEY] = $token;
        return $token;
    }

    public function validate(?string $token): bool
    {
        if (!isset($_SESSION[self::SESSION_KEY]) || !is_string($_SESSION[self::SESSION_KEY])) {
            return false;
        }
        if ($token === null || $token === '') {
            return false;
        }
        return hash_equals($_SESSION[self::SESSION_KEY], $token);
    }
}
