# DIC Agent

Génération automatisée de **Diplomatic Clearance (DIC)** à destination des attachés défense.

## Périmètre

- Couverture **mondiale** : aéroports OACI, types d'avions ICAO, frontières d'États, NAVAID/waypoints.
- Deux formats supportés : **FRA court** (aller-retour standard) et **ICAO long** (multi-legs, colonnes étendues).
- Application **locale** (Streamlit + SQLite). Aucune donnée sortante.

## Le flux

1. L'utilisateur choisit / crée des **profils** réutilisables (appareil, équipage, POC, opérateur).
2. Pour chaque **leg**, il saisit : date + EOBT (UTC), origin / destination ICAO, route texte ICAO (ex. `TYE POLTO LAG L433 IBA R778 TEGDA MNA`), FL, TAS.
3. Le **moteur de route** :
   - tokenise la route, géolocalise chaque waypoint via la base locale,
   - calcule les distances grand-cercle cumulées et les heures de passage à TAS donnée,
   - intersecte la trace avec les polygones d'États (Natural Earth) pour produire **entry/exit par État**,
   - propose R / N / L automatiquement (override manuel possible).
4. **Preview** live du tableau Appendix 1 avec warnings rouges pour tout waypoint non résolu.
5. Génération `.docx` via `docxtpl` à partir des templates `templates/dic_fra_short.docx` ou `templates/dic_icao_long.docx`.

## Démarrage

```bash
pip install -r requirements.txt
python -m app.seed_db           # première fois : importe aéroports / types / waypoints / pays
streamlit run app/main.py
```

## Données seed

- **Aéroports** : OurAirports `airports.csv` (~80 000 entrées, gratuit).
- **Types d'avions** : ~250 types ICAO courants (militaire + civil) avec cruise TAS, ceiling, range. Extensible par l'utilisateur dans l'UI.
- **Waypoints / NAVAID** : OurAirports `navaids.csv` (~30 000 entrées). Tout point inconnu peut être ajouté à la volée (saisie nom + lat/lon, persisté).
- **Frontières d'États** : Natural Earth Admin-0 `ne_50m_admin_0_countries`.

Les fichiers sources sont téléchargés au premier `seed_db` (voir `seeds/`).

## Structure

```
dic-agent/
├── app/
│   ├── main.py            # Streamlit entrypoint
│   ├── db.py              # SQLite schema + helpers
│   ├── seed_db.py         # Import aéroports / types / waypoints / pays
│   ├── route_engine.py    # Parse route, distances, ETO, entry/exit par État
│   ├── docx_generator.py  # Rendu docxtpl FRA / ICAO
│   └── ui/                # Onglets Streamlit
├── templates/
│   ├── dic_fra_short.docx
│   └── dic_icao_long.docx
├── seeds/                 # CSV/GeoJSON téléchargés
├── data/dic.sqlite        # Base locale (gitignoré)
└── tests/
```

## État

Phase 1 — squelette + moteur + template FRA. Phase 2 — ICAO long + bibliothèque de routes habituelles + diff d'amendement.
