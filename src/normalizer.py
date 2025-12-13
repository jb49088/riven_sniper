CANONICAL_STATS = {
    "ammo_max",
    "cold",
    "combo_count_chance",
    "combo_duration",
    "corpus_damage",
    "crit_chance",
    "crit_damage",
    "damage",
    "electric",
    "finisher",
    "fire_rate",
    "grineer_damage",
    "heat",
    "heavy_efficiency",
    "impact",
    "infested_damage",
    "initial_combo",
    "magazine",
    "multishot",
    "projectile_speed",
    "punch_through",
    "puncture",
    "range",
    "recoil",
    "reload_speed",
    "slash",
    "slide_crit_chance",
    "status_chance",
    "status_duration",
    "toxin",
    "zoom",
}

RIVEN_MARKET_TO_CANONICAL = {
    "Ammo": "ammo_max",
    "ComboGainExtra": "combo_count_chance",
    "Corpus": "corpus_damage",
    "Grineer": "grineer_damage",
    "Infested": "infested_damage",
    "Cold": "cold",
    "Combo": "combo_duration",
    "CritChance": "crit_chance",
    "Slide": "slide_crit_chance",
    "CritDmg": "crit_damage",
    "Damage": "damage",
    "Electric": "electric",
    "Heat": "heat",
    "Finisher": "finisher",
    "Speed": "fire_rate",
    "Flight": "projectile_speed",
    "InitC": "initial_combo",
    "Impact": "impact",
    "Magazine": "magazine",
    "ComboEfficiency": "heavy_efficiency",
    "Multi": "multishot",
    "Toxin": "toxin",
    "Punch": "punch_through",
    "Puncture": "puncture",
    "Reload": "reload_speed",
    "Range": "range",
    "Slash": "slash",
    "StatusC": "status_chance",
    "StatusD": "status_duration",
    "Recoil": "recoil",
    "Zoom": "zoom",
}

WARFRAME_MARKET_TO_CANONICAL = {
    "ammo_maximum": "ammo_max",
    "chance_to_gain_extra_combo_count": "combo_count_chance",
    "chance_to_gain_combo_count": "combo_count_chance",
    "damage_vs_corpus": "corpus_damage",
    "damage_vs_grineer": "grineer_damage",
    "damage_vs_infested": "infested_damage",
    "cold_damage": "cold",
    "combo_duration": "combo_duration",
    "critical_chance": "crit_chance",
    "critical_chance_on_slide_attack": "slide_crit_chance",
    "critical_damage": "crit_damage",
    "base_damage_/_melee_damage": "damage",
    "electric_damage": "electric",
    "heat_damage": "heat",
    "finisher_damage": "finisher",
    "fire_rate_/_attack_speed": "fire_rate",
    "projectile_speed": "projectile_speed",
    "channeling_damage": "initial_combo",
    "impact_damage": "impact",
    "magazine_capacity": "magazine",
    "channeling_efficiency": "heavy_efficiency",
    "multishot": "multishot",
    "toxin_damage": "toxin",
    "punch_through": "punch_through",
    "puncture_damage": "puncture",
    "reload_speed": "reload_speed",
    "range": "range",
    "slash_damage": "slash",
    "status_chance": "status_chance",
    "status_duration": "status_duration",
    "recoil": "recoil",
    "zoom": "zoom",
}


def normalize_stat_name(stat, source):
    if source == "riven.market":
        return RIVEN_MARKET_TO_CANONICAL.get(stat)
    elif source == "warframe.market":
        return WARFRAME_MARKET_TO_CANONICAL.get(stat)
    return None


def normalize_riven_stats(stat1, stat2, stat3, stat4, source):
    normalized = []

    for stat in [stat1, stat2, stat3, stat4]:
        if stat == "":
            normalized.append("")
        else:
            canonical = normalize_stat_name(stat, source)
            if canonical is None:
                # Invalid/unmapped stat found
                return None
            normalized.append(canonical)

    return tuple(normalized)


def sort_positive_stats(stat1, stat2, stat3, stat4):
    positives = [s for s in [stat1, stat2, stat3] if s]
    positives.sort()
    while len(positives) < 3:
        positives.append("")
    return tuple(positives + [stat4])


def normalize(stat1, stat2, stat3, stat4, source):
    # Normalize stat names
    normalized = normalize_riven_stats(stat1, stat2, stat3, stat4, source)
    if normalized is None:
        return None

    # Sort positive stats
    return sort_positive_stats(*normalized)
