<?php

declare(strict_types=1);

namespace App\Db;

final class ConnectionManager
{
    /** @var array<string,mixed> */
    private array $config;

    /** @var array<string,\PDO> */
    private array $cache = [];

    /** @param array<string,mixed> $config */
    public function __construct(array $config)
    {
        $this->config = $config;
    }

    /**
        * @return array<string,array{host:string,port:int,username:string,password:string,database:string,charset?:string}>
        */
    public function characterProfiles(): array
    {
        /** @var array<string,array{host:string,port:int,username:string,password:string,database:string,charset?:string}> $profiles */
        $profiles = $this->config['character_dbs'] ?? [];
    
        return $profiles;
    }

    public function getCharacter(string $profileName): \PDO
    {
        if (isset($this->cache['char:' . $profileName]))
        {
            return $this->cache['char:' . $profileName];
        }

        $profiles = $this->characterProfiles();
        if (!isset($profiles[$profileName]))
        {
            throw new \RuntimeException('Unknown character DB profile: ' . $profileName);
        }

        $pdo = PdoFactory::make($profiles[$profileName]);
        $this->cache['char:' . $profileName] = $pdo;

        return $pdo;
    }
}
