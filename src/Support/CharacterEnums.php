<?php

declare(strict_types=1);

namespace App\Support;

final class CharacterEnums
{
    /** @var array<int,string> */
    private const RACES = [
        1 => 'Human',
        2 => 'Orc',
        3 => 'Dwarf',
        4 => 'Night Elf',
        5 => 'Undead',
        6 => 'Tauren',
        7 => 'Gnome',
        8 => 'Troll',
    ];

    /** @var array<int,string> */
    private const GENDERS = [
        0 => 'Male',
        1 => 'Female',
    ];

    /** @var array<int,string> */
    private const CLASSES = [
        1 => 'Warrior',
        2 => 'Paladin',
        3 => 'Hunter',
        4 => 'Rogue',
        5 => 'Priest',
        7 => 'Shaman',
        8 => 'Mage',
        9 => 'Warlock',
        11 => 'Druid',
    ];

    public static function raceName(int $raceId): string
    {
        return self::RACES[$raceId] ?? ('Unknown (' . $raceId . ')');
    }

    public static function className(int $classId): string
    {
        return self::CLASSES[$classId] ?? ('Unknown (' . $classId . ')');
    }

    public static function genderName(int $genderId): string
    {
        return self::GENDERS[$genderId] ?? ('Unknown (' . $genderId . ')');
    }
}
