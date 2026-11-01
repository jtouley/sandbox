DBT := uv run dbt
FLAGS := --profiles-dir .

.PHONY: setup build build-md docs clean

setup:     ## Create venv and install dbt-duckdb (Python 3.12)
	uv sync

build:     ## Build + test everything into a local DuckDB file
	$(DBT) build $(FLAGS)

build-md:  ## Build + test everything in MotherDuck (needs MOTHERDUCK_TOKEN)
	$(DBT) build $(FLAGS) --target md

docs:      ## Generate and serve dbt docs (lineage graph) on :8080
	$(DBT) docs generate $(FLAGS) && $(DBT) docs serve $(FLAGS)

clean:
	$(DBT) clean $(FLAGS); rm -f halloween_candy.duckdb*
