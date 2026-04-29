# APEX Enrichment Agent

Agent d'enrichissement d'emails pour les opérateurs aériens cargo-capables.

## Mission

Garantir que chaque opérateur cargo-capable a un email de contact charter
vérifié, **avec zéro hallucination** : tout email retenu est littéralement
présent dans la source HTML fetchée, avec URL et snippet de preuve sauvés.

Pour chaque opérateur :
- **Sans email connu** → mode *discovery* : on scrape le site officiel et
  les pages candidates (`/contact`, `/cargo`, `/charter`, etc.).
- **Avec email connu** (depuis `operators_list.csv`) → mode *verify* : on
  re-scrape pour confirmer que l'email est toujours sur le site officiel,
  et on cherche en parallèle un éventuel meilleur (`cargo@` > `info@`).

## Architecture

- **Python 3.11+** pour le scraping (déterministe, rate-limité, respectueux
  de `robots.txt`)
- **Anthropic Claude Sonnet** uniquement pour les cas ambigus (HTML mal
  structuré), avec citation textuelle obligatoire
- **Sortie CSV** (`drafts.csv`) — pas de base de données, validation humaine
  dans Excel/Sheets en remplissant la colonne `validation_status`

## Démarrage

```bash
pip install -r requirements.txt
cp .env.example .env  # remplir ANTHROPIC_API_KEY (USER_AGENT_CONTACT optionnel)

# Régénérer le BACKLOG depuis operators_list.csv (à faire si la source change)
python scripts/regenerate_backlog.py

# Test sur 10 opérateurs
python scripts/run_enrichment.py --limit 10

# Run complet
python scripts/run_enrichment.py
```

## Workflow de validation

1. Le pipeline écrit `drafts.csv` à la racine.
2. Tu ouvres `drafts.csv` dans Sheets/Excel.
3. Pour chaque ligne, tu remplis la colonne `validation_status` :
   `approved` / `rejected` / vide.
4. Re-runs ultérieurs : tes annotations sont préservées (clé
   `(operator_name, email, source_url)`).

## Structure des fichiers

```
apex-enrichment-agent/
├── operators_list.csv          # Source brute (1 ligne par aéronef)
├── BACKLOG.csv                 # 315 opérateurs cargo-capables, généré
├── drafts.csv                  # Sortie du pipeline, à valider à la main
├── lib/
│   ├── scraper.py              # HTTP GET, robots.txt, rate limit
│   ├── email_extractor.py      # Regex + verbatim assertion
│   ├── mx_check.py             # dnspython MX lookup
│   └── llm_assist.py           # Claude Sonnet pour cas ambigus
└── scripts/
    ├── regenerate_backlog.py   # Reconstruit BACKLOG depuis operators_list
    └── run_enrichment.py       # Pipeline principal
```

Voir `CLAUDE.md` pour les règles dures et conventions.
