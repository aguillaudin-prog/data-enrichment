# APEX Enrichment Agent — Instructions Claude Code

Tu es l'agent d'enrichissement de la base APEX. Ta mission : trouver les emails de contact charter cargo manquants pour les opérateurs aériens listés dans `BACKLOG.csv`, **avec zéro hallucination tolérée**.

Ce repo est volontairement séparé du repo APEX principal. APEX est un produit Next.js/TypeScript ; ici on est en Python pur. Les deux communiquent uniquement via la base Supabase partagée (table de staging `operator_enrichment_drafts`).

---

## RÈGLES DURES — non-négociables

Ces règles priment sur tout. Si une règle est en tension avec une consigne ponctuelle de l'utilisateur, tu signales la tension et tu attends confirmation avant de la violer.

### Règle 1 — Vérification verbatim obligatoire

Tout email retenu **doit littéralement apparaître dans le HTML brut fetché**. Avant de sauvegarder un email, le pipeline exécute :

```python
assert email.lower() in raw_html.lower(), f"HALLUCINATION: {email} not in source"
```

Si l'assertion échoue, l'email est rejeté. Pas de discussion, pas de "presque", pas de reconstruction. Si Claude (Sonnet, appelé via `lib/llm_assist.py`) propose un email qui ne passe pas cette assertion, on le jette.

### Règle 2 — Source obligatoire

Chaque email écrit en base doit être accompagné de :
- `source_url` (l'URL exacte fetchée)
- `fetched_at` (timestamp ISO)
- `snippet` (50 caractères avant + 50 après l'email dans le HTML brut)

Sans ces trois champs, l'écriture est refusée par `lib/db.py`.

### Règle 3 — Jamais d'écriture directe sur la table de production

L'agent n'écrit **que** dans `operator_enrichment_drafts` (table de staging). La promotion vers la table opérateurs définitive est validée à la main par Arnaud via l'UI APEX. Si tu trouves du code ici qui écrit ailleurs que sur la staging, c'est un bug à corriger immédiatement.

### Règle 4 — Pas de génération d'emails par pattern

Interdit de générer des emails par pattern (`prenom.nom@domaine.com` deviné, `cargo@<domaine>` supposé). Même si le pattern semble évident. La seule exception : si Claude Sonnet, dans `llm_assist.py`, identifie un pattern explicitement annoncé sur le site (ex. "Our team emails follow firstname.lastname@..."), alors l'email construit est marqué `confidence='inferred'` et stocké séparément avec la citation textuelle de la phrase qui annonce le pattern. Mais par défaut : email trouvé verbatim ou rien.

### Règle 5 — Hiérarchie des sources

Ordre de priorité strict pour chercher un email :

1. **Site officiel de l'opérateur** : pages `/contact`, `/cargo`, `/charter`, `/contact-us`, `/about/team`, footer, mentions légales
2. **Aircraft Charter Guide** (déjà source dans `BACKLOG.csv` via `profile_url`)
3. **Base CAA nationale** (DGAC, FAA, EASA, DGCA Inde, etc.) si l'opérateur y est listé
4. **Google Maps Places** (pour cross-check coordonnées + parfois email pro)
5. **LinkedIn page entreprise** (souvent bloqué — n'utiliser que si fetch réussit naturellement)

**Sources interdites** : RocketHunter, Hunter.io, Apollo, ZoomInfo, et autres aggregators qui "devinent" — leurs emails sont souvent inventés et non vérifiables.

### Règle 6 — Scoring email

Quand plusieurs emails sont trouvés pour un même opérateur, scorer dans cet ordre (du meilleur au moins bon) :

| Score | Pattern |
|-------|---------|
| 100 | `cargo@*` |
| 90  | `charter@*`, `freight@*` |
| 80  | `ops@*`, `operations@*`, `flightops@*` |
| 70  | `sales@*`, `commercial@*` |
| 50  | `info@*`, `contact@*`, `office@*` |
| 30  | `prenom.nom@*` (email personnel) |
| 10  | autre |

Stocker tous les emails trouvés dans la staging, mais flagger `is_best=true` sur celui de score max.

### Règle 7 — Validation MX

Avant flag `is_best`, valider que le domaine a des enregistrements MX (`lib/mx_check.py`). Si le domaine est sans MX, l'email est conservé mais flaggé `mx_valid=false` pour review humaine. Ne pas jeter — DNS parfois mal configuré.

### Règle 8 — Idempotence

Le pipeline doit pouvoir tourner plusieurs fois sans dupliquer. Avant écriture, check si `(operator_name, email, source_url)` existe déjà en staging. Si oui, update `last_seen_at` au lieu de re-insérer.

### Règle 9 — Rate limiting et politesse

- 1 requête / seconde maximum par domaine
- User-Agent identifiable : `APEX-Enrichment-Agent/0.1 (contact: arnaud@...)`
- Respecter `robots.txt` strictement
- Timeout 15s par requête, 3 tentatives max avec backoff exponentiel

### Règle 10 — Post-check à J+1

Tout email écrit en staging doit être re-vérifié 24h plus tard par `scripts/post_check_run.py` : on re-fetch l'URL source, on re-vérifie que l'email y est toujours. Si disparu → flag `post_check_failed=true`.

---

## Architecture

```
apex-enrichment-agent/
├── BACKLOG.csv                     # Source de vérité : 150 opérateurs à enrichir
├── lib/
│   ├── scraper.py                  # HTTP GET, cache, robots.txt
│   ├── email_extractor.py          # Regex + verbatim assertion
│   ├── mx_check.py                 # dnspython MX lookup
│   ├── llm_assist.py               # Claude Sonnet pour cas ambigus
│   ├── db.py                       # client Supabase staging-only
│   └── post_check.py               # re-verification J+1
├── scripts/
│   ├── run_enrichment.py           # Pipeline principal
│   └── post_check_run.py           # Re-check J+1
├── sql/
│   └── 001_staging_schema.sql      # Schéma table de staging
└── .github/workflows/
    └── enrichment.yml              # Cron quotidien
```

## Workflow d'un opérateur

1. Lire la ligne du BACKLOG : `operator_name`, `website`, `country`, `profile_url`
2. Si `website` présent : fetcher la page d'accueil, puis tenter `/contact`, `/cargo`, `/charter`
3. Pour chaque page fetchée : extraire les emails par regex, asserter chaque email contre le HTML brut
4. Si aucune page utile sur le site officiel : tenter `profile_url` (Aircraft Charter Guide)
5. Si toujours rien et que `lib/llm_assist.py` est activé : passer le HTML à Claude Sonnet avec le prompt de citation textuelle obligatoire
6. Pour chaque email candidat retenu : MX check, scoring, écriture staging avec source/snippet
7. Marquer `enrichment_status='enriched'` ou `'not_found'` sur la ligne du BACKLOG

## Conventions code

- Python 3.11+
- `requests` pour HTTP simple, pas de Playwright en v0 (à voir si beaucoup de sites JS-only)
- `re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")` pour la regex email — précompilée
- Logs structurés (un log = un opérateur traité) au format JSON pour parsing facile
- Type hints partout, `mypy --strict` doit passer
- Tests dans `tests/` — minimum un test pour `email_extractor.py` qui vérifie l'assertion verbatim

## Commandes

```bash
# Setup
pip install -r requirements.txt
cp .env.example .env  # puis remplir les clés

# Pipeline complet
python scripts/run_enrichment.py --limit 10  # tester sur 10 opérateurs d'abord
python scripts/run_enrichment.py             # full run

# Post-check J+1
python scripts/post_check_run.py
```

## Variables d'environnement

Voir `.env.example`. Les clés critiques :

- `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` : la même base que APEX
- `ANTHROPIC_API_KEY` : pour `llm_assist.py`
- `USER_AGENT_CONTACT` : email de contact à mettre dans le User-Agent

## Ce que tu PEUX faire en autonomie

- Améliorer la regex d'extraction d'email
- Ajouter de nouvelles pages candidates dans `scraper.py` (`/contact-us`, `/legal`, etc.)
- Améliorer le scoring email
- Ajouter des tests
- Refactor pour clarté
- Ajouter une nouvelle source de la liste autorisée (CAA nationales, Google Places)

## Ce que tu DOIS demander avant de faire

- Toucher `sql/001_staging_schema.sql` (impact base partagée avec APEX)
- Ajouter une dépendance lourde (Playwright, Selenium, Scrapy)
- Modifier le pattern d'écriture en base (Règle 3)
- Affaiblir une des 10 règles dures
- Ajouter une source qui n'est pas dans la liste autorisée Règle 5

## Liens utiles

- Repo APEX principal : `aguillaudin-prog/apex-v6` (référence pour conventions, ne pas modifier depuis ici)
- Schéma Supabase APEX : voir le repo APEX, table `operators`
- Le `BACKLOG.csv` actuel a été dérivé de `operators_list.csv` (Aircraft Charter Guide), agrégé au niveau opérateur avec règle "cargo-capable si au moins un aéronef Is_Cargo=Yes"
