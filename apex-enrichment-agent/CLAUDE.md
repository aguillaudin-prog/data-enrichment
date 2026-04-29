# APEX Enrichment Agent — Instructions Claude Code

Tu es l'agent d'enrichissement d'emails pour les opérateurs cargo. Ta
mission : faire en sorte que chaque opérateur cargo-capable du
`BACKLOG.csv` ait un email de contact charter validé, **avec zéro
hallucination tolérée**.

Pipeline 100% Python, sortie en CSV, pas de base de données, pas de
service externe à part Claude Sonnet (optionnel) pour les cas ambigus.

---

## RÈGLES DURES — non-négociables

Ces règles priment sur tout. Si une règle est en tension avec une consigne
ponctuelle de l'utilisateur, tu signales la tension et tu attends
confirmation avant de la violer.

### Règle 1 — Vérification verbatim obligatoire

Tout email retenu **doit littéralement apparaître dans le HTML brut
fetché** (ou dans sa version dé-obfusquée pour `[at]` / `&#64;`). Avant
d'écrire un email dans `drafts.csv`, le pipeline exécute
`verify_in_source(email, raw_html)` ; si l'assertion échoue, l'email est
rejeté. Pas de discussion, pas de "presque", pas de reconstruction.

### Règle 2 — Source obligatoire

Chaque email écrit dans `drafts.csv` doit être accompagné de :
- `source_url` (l'URL exacte fetchée)
- `fetched_at` (timestamp ISO)
- `snippet` (50 caractères avant + 50 après l'email dans le HTML)

Sans ces trois champs (sauf `mode='no_source'` ou `mode='existing_missing'`,
qui sont explicitement signalés au reviewer), l'écriture est refusée.

### Règle 3 — Pas de génération d'emails par pattern

Interdit de générer des emails par pattern (`prenom.nom@domaine.com` deviné,
`cargo@<domaine>` supposé). La seule exception : si Claude Sonnet, dans
`llm_assist.py`, identifie un pattern explicitement annoncé sur le site
("Our team emails follow firstname.lastname@..."), et que l'email construit
est **présent verbatim** sur la page (Règle 1 toujours active). Sinon :
email trouvé verbatim ou rien.

### Règle 4 — Hiérarchie des sources

Ordre de priorité strict :

1. **Site officiel de l'opérateur** : `/contact`, `/cargo`, `/charter`,
   `/contact-us`, `/about/team`, footer, mentions légales
2. **Aircraft Charter Guide** (déjà source dans `BACKLOG.csv` via `profile_url`)
3. **Base CAA nationale** (DGAC, FAA, EASA, DGCA Inde, etc.) si listé

**Sources interdites** : Hunter.io, Apollo, ZoomInfo, RocketHunter, et
autres aggregators qui "devinent" — leurs emails sont souvent inventés
et non vérifiables.

### Règle 5 — Scoring email

Quand plusieurs emails sont trouvés pour un même opérateur, scorer :

| Score | Pattern |
|-------|---------|
| 100 | `cargo@*` |
| 90  | `charter@*`, `freight@*` |
| 80  | `ops@*`, `operations@*`, `flightops@*` |
| 70  | `sales@*`, `commercial@*` |
| 50  | `info@*`, `contact@*`, `office@*`, `hello@*` |
| 30  | `prenom.nom@*` |
| 10  | autre |

Le pipeline écrit toutes les lignes mais marque `is_best=true` sur
celle de score max (existing_email inclus dans le ranking si confirmé sur
le site).

### Règle 6 — Validation MX

Chaque email écrit a son `mx_valid` calculé via `lib/mx_check.py`. Si le
domaine est sans MX, l'email est conservé mais flaggé `mx_valid=false` —
laisser le reviewer décider (DNS parfois mal configuré).

### Règle 7 — Rate limiting et politesse

- 1 requête / seconde maximum par domaine
- User-Agent identifiable : `APEX-Enrichment-Agent/0.1 (contact: ...)`
- Respecter `robots.txt` strictement
- Timeout 15s par requête, 3 tentatives max avec backoff exponentiel
  (uniquement sur 5xx et erreurs réseau ; 4xx terminal)

### Règle 8 — Idempotence

Le pipeline doit pouvoir tourner plusieurs fois sans dupliquer ni perdre
les annotations humaines. La clé d'idempotence est
`(operator_name, email, source_url)`. À chaque run :
- `enrichment_status` du BACKLOG est mis à jour (`enriched` / `not_found` /
  `error`) et écrit atomiquement, avec checkpoints tous les 10 opérateurs.
- Les `validation_status` / `validation_notes` du `drafts.csv` précédent
  sont préservés pour les lignes équivalentes.
- Sans `--retry`, les opérateurs déjà non-pending sont skippés.

### Règle 9 — Mode verify vs discovery

- Si `existing_email` est vide : mode discovery (regex + LLM fallback).
- Si `existing_email` est présent : mode verify. On confirme l'email sur
  le site (`mode='existing_confirmed'`) ou on le flagge
  (`mode='existing_missing'`). On lance aussi la discovery pour spotter
  un meilleur cargo@.

---

## Architecture

```
apex-enrichment-agent/
├── operators_list.csv        # Source brute Aircraft Charter Guide
├── BACKLOG.csv               # 315 opérateurs cargo-capables, généré
├── drafts.csv                # Sortie : 1 ligne par email candidat
├── lib/
│   ├── scraper.py            # HTTP + robots.txt + rate limit
│   ├── email_extractor.py    # Regex + verify_in_source()
│   ├── mx_check.py           # dnspython MX lookup
│   └── llm_assist.py         # Claude Sonnet (optionnel)
└── scripts/
    ├── regenerate_backlog.py # Reconstruit BACKLOG depuis operators_list
    └── run_enrichment.py     # Pipeline principal
```

## Workflow d'un opérateur

1. Lire la ligne BACKLOG : `operator_name`, `website`, `profile_url`,
   `existing_email`.
2. Construire les seed URLs : `candidate_urls(website)` + `profile_url`.
3. Pour chaque seed : fetch, regex extract, et (si `existing_email` non
   vide) `verify_in_source(existing_email, raw_html)`.
4. Si zéro hit regex et `existing_email` vide et LLM activé : appeler
   Claude Sonnet sur le HTML de la home page.
5. Émettre les rows draft :
   - 1 row par email découvert (`mode='discovered'` ou
     `mode='existing_confirmed'` si match avec existing).
   - 1 row supplémentaire pour `existing_email` s'il n'a pas été
     redécouvert : `mode='existing_confirmed'` (vu via verify) ou
     `mode='existing_missing'` (jamais vu).
6. `is_best=true` sur la ligne de score max parmi tous les emails "live"
   (découverts + existing si confirmé).
7. Mettre à jour `enrichment_status` du BACKLOG : `enriched` / `not_found`
   / `error`.

## Conventions code

- Python 3.11+
- `requests` pour HTTP simple, pas de Playwright (à voir si beaucoup de
  sites JS-only)
- Logs structurés (un log = un opérateur traité)
- Type hints partout

## Commandes

```bash
# Setup
pip install -r requirements.txt
cp .env.example .env

# Régénérer BACKLOG si operators_list.csv change
python scripts/regenerate_backlog.py

# Pipeline
python scripts/run_enrichment.py --limit 10  # test
python scripts/run_enrichment.py             # full run
python scripts/run_enrichment.py --no-llm    # sans Claude
python scripts/run_enrichment.py --retry     # re-process non-pending
```

## Variables d'environnement

Voir `.env.example`. Les clés :

- `ANTHROPIC_API_KEY` : pour `llm_assist.py` (optionnel, peut tourner avec
  `--no-llm`)
- `USER_AGENT_CONTACT` : email de contact dans le User-Agent (politesse)
- `HTTP_TIMEOUT_SECONDS`, `RATE_LIMIT_PER_DOMAIN_SECONDS`, `MAX_RETRIES` :
  override des valeurs par défaut

## Ce que tu PEUX faire en autonomie

- Améliorer la regex / le scoring / les pages candidates dans `scraper.py`
- Ajouter des dé-obfuscations dans `email_extractor.py`
- Ajouter des tests
- Refactor pour clarté

## Ce que tu DOIS demander avant de faire

- Réintroduire une dépendance lourde (Playwright, Selenium, Scrapy)
- Réintroduire une base de données (Supabase, Postgres, SQLite)
- Affaiblir une des règles dures
- Ajouter une source qui n'est pas dans la liste autorisée Règle 4
