# APEX Enrichment Agent

Agent d'enrichissement de contacts pour les opérateurs aériens cargo, alimentant la base APEX.

## Mission

Compléter les emails de contact charter/cargo manquants dans la base opérateurs, en scrappant uniquement des sources publiques et **avec garantie zéro hallucination** : tout email retenu est littéralement présent dans la source fetchée, avec URL et snippet de preuve sauvegardés.

## Architecture

- **Python 3.11+** pour le scraping (déterministe, rate-limité, respectueux de `robots.txt`)
- **Anthropic Claude Sonnet** uniquement pour les cas ambigus (parsing HTML mal structuré), avec citation textuelle obligatoire
- **Supabase** pour la persistance (table de staging séparée de la prod APEX)
- **GitHub Actions** pour l'exécution cron

## Output

L'agent ne touche jamais la table opérateurs de production. Il écrit dans `operator_enrichment_drafts` un brouillon validé manuellement via l'UI APEX avant promotion.

## Démarrage

Voir `CLAUDE.md` pour les règles de l'agent et les commandes.

```bash
pip install -r requirements.txt
cp .env.example .env  # remplir les clés
python scripts/run_enrichment.py --limit 10
```

## Source des données

`BACKLOG.csv` : 150 opérateurs cargo-capables sans email, dérivés d'Aircraft Charter Guide et agrégés au niveau opérateur (un opérateur est cargo-capable si au moins un de ses aéronefs l'est).
