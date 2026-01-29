<?php

declare(strict_types=1);

namespace App\Db;

final class PdoFactory
{
    /**
        * @param array{host:string,port:int,username:string,password:string,database:string,charset?:string} $cfg
        */
    public static function make(array $cfg): \PDO
    {
        $charset = $cfg['charset'] ?? 'utf8mb4';
        $dsn = sprintf(
            'mysql:host=%s;port=%d;dbname=%s;charset=%s',
            $cfg['host'],
            (int)$cfg['port'],
            $cfg['database'],
            $charset
        );

        $pdo = new \PDO($dsn, $cfg['username'], $cfg['password'], [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
            \PDO::ATTR_EMULATE_PREPARES => false,
        ]);

        // For consistent results
        $pdo->exec("SET SESSION sql_mode = 'STRICT_ALL_TABLES'");
        return $pdo;
    }
}
