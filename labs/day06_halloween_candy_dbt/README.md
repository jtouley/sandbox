# 🎃 Ari's Halloween Candy Haul: dbt + MotherDuck

A small dbt project that turns Ari's 2026 haul (129 pieces, 34 kinds) into tables you can chart,
filter, and play with in [MotherDuck](https://motherduck.com), the cloud version of DuckDB.

It runs **locally with no account** (a DuckDB file) and against **MotherDuck** with one flag.

```
seeds/raw_candy_haul.csv ──► stg_candy_haul ──┐
                                              ├─► int_candy_enriched ─► fct_candy_haul ─► mart_* tables
seeds/candy_attributes.csv ► stg_candy_attributes ┘
```

| Layer | Model | What it's for |
|---|---|---|
| seed | `raw_candy_haul` | The haul list, exactly as written down |
| seed | `candy_attributes` | Hand-made lookup: category, maker, nut allergens, **estimated** sugar/calories per fun-size piece, `trade_value` 1–5 |
| mart | `fct_candy_haul` | One row per candy. **Start here.** Counts, % of haul, rank, parent tax, trade points |
| mart | `mart_haul_summary` | One-row KPI card (total pieces, % chocolate, sugar kg, …) |
| mart | `mart_category_summary` | Chocolate vs. fruity vs. sour vs. … |
| mart | `mart_manufacturer_summary` | Mars vs. Hershey "market share" |
| mart | `mart_allergen_report` | Which pieces contain peanuts / tree nuts |
| mart | `mart_candy_rationing` | Eats the best candy first at N pieces/day: what day does it run out? |

> Nutrition numbers and `trade_value` are rough guesses, not label data. Edit
> `seeds/candy_attributes.csv`; deciding the trade values is a good job for Ari.

---

## 1. Run it locally first (no account needed)

Requires [uv](https://docs.astral.sh/uv/). This lab has **its own** Python 3.12 environment,
because dbt doesn't support the repo-root Python 3.14 yet.

```bash
cd labs/day06_halloween_candy_dbt
make setup     # uv sync → .venv with dbt-core + dbt-duckdb
make build     # seeds, models, and 22 tests → halloween_candy.duckdb
```

Poke at the results:

```bash
uv run python -c "import duckdb; print(duckdb.connect('halloween_candy.duckdb').sql('from marts.mart_category_summary'))"
```

(Or open `halloween_candy.duckdb` in the [DuckDB CLI](https://duckdb.org/docs/installation/) / DBeaver.)

---

## 2. Set up MotherDuck

1. **Create an account.** Go to <https://app.motherduck.com> and sign up with Google, GitHub,
   or email. The free plan is plenty for this project; check
   <https://motherduck.com/pricing> for current limits.
2. **Create an access token.** In the MotherDuck UI, click your org/profile menu (top left) →
   **Settings** → **Access Tokens** → **Create token**. Give it a name like `dbt-candy`
   and copy the token. It is only shown once.
3. **Put the token in your environment.** It's a password, so never commit it. This repo's
   `.envrc` is gitignored, so add it there (direnv loads it automatically):

   ```bash
   # .envrc (repo root)
   export MOTHERDUCK_TOKEN="paste-token-here"
   ```

   Then run `direnv allow`. Or just `export MOTHERDUCK_TOKEN=...` in your shell for a session.
4. **Create the database once.** dbt connects to `md:halloween_candy`, which must exist:
   - in the MotherDuck UI: click **+** next to "Attached databases" → **Create database** →
     name it `halloween_candy`, **or**
   - from the terminal:
     `uv run python -c "import duckdb; duckdb.connect('md:').sql('create database if not exists halloween_candy')"`
5. **Build into MotherDuck:**

   ```bash
   make build-md          # = uv run dbt build --profiles-dir . --target md
   ```

   Refresh the MotherDuck UI and you'll see `halloween_candy` with `raw`, `staging`,
   and `marts` schemas.

If dbt can't find the token it opens a browser login instead. That works too, but the token is
smoother.

---

## 3. Visualize & play in MotherDuck

Open a notebook in the MotherDuck UI and try:

```sql
use halloween_candy;

-- Top 10: results have a built-in chart toggle, so pick a bar chart
from marts.fct_candy_haul select display_name, piece_count order by piece_count desc limit 10;

-- Pie / bar of categories
from marts.mart_category_summary;

-- Daily candy countdown: line chart of pieces left by day
with daily as (
    select day_number, eat_date, count(*) as eaten
    from marts.mart_candy_rationing group by all
)
select *, sum(eaten) over () - sum(eaten) over (order by day_number) as pieces_left
from daily order by day_number;
```

Click a column header in results to open the **Column Explorer**, which shows instant
histograms and value distributions. More ready-to-run queries live in `analyses/`
(`uv run dbt compile --profiles-dir .` renders them to `target/compiled/.../analyses/`).

You can also ask MotherDuck's AI in plain English ("which manufacturer gave us the most
chocolate?"). It sees these tables.

---

## 4. Knobs to play with

Change project variables at build time without editing SQL:

```bash
# Parents get greedy: 25% chocolate tax, and Ari is allowed 5 pieces a day
uv run dbt build --profiles-dir . --vars '{parent_tax_pct: 0.25, daily_piece_limit: 5}'
```

| Var | Default | Affects |
|---|---|---|
| `parent_tax_pct` | `0.10` | `parent_tax_pieces` / `pieces_kept` (chocolate only, rounded per candy) |
| `daily_piece_limit` | `3` | How many days `mart_candy_rationing` lasts |
| `trick_or_treater`, `halloween_year` | `Ari`, `2026` | Labels and dates |

Other ideas:
- **Next year:** add a `year` column to the haul seed (or a second seed) and compare years.
- **Siblings / friends:** add a `trick_or_treater` column and build a trading-market model.
- **New candy:** add it to *both* CSVs. If you forget the attributes row, the `not_null`
  test on `int_candy_enriched.display_name` fails and tells you.
- `make docs` opens dbt's docs site with the lineage graph.

---

## Project layout

```
day06_halloween_candy_dbt/
├── dbt_project.yml        # vars + per-layer schema/materialization
├── profiles.yml           # targets: local (DuckDB file) and md (MotherDuck)
├── pyproject.toml         # its own uv env (Python 3.12)
├── seeds/                 # raw_candy_haul.csv, candy_attributes.csv
├── models/{staging,intermediate,marts}/
├── macros/                # pct(), schema naming without target prefixes
├── tests/                 # positive-count generic test, "no pieces lost" check
└── analyses/              # handy ad-hoc queries
```
