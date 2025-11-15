#!/usr/bin/env python
"""
CLI entrypoint for config-driven pipeline.

Usage:
    # Run with default config
    python run_pipeline.py

    # Override config file
    python run_pipeline.py --config config/custom.yaml

    # Override specific values
    python run_pipeline.py --source polars_synthetic --mode speed --scale 1000000

    # Start Prefect server and serve flow
    python run_pipeline.py --serve

    # Start Prefect server and serve flow with custom cron
    python run_pipeline.py --serve --cron "0 */6 * * *"
"""

import argparse
import sys
from pathlib import Path

# Add labs to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from day05_configdriven.flows.pipeline import run_pipeline
from day05_configdriven.src.utils.logging_config import setup_logging


def main():
    parser = argparse.ArgumentParser(
        description="Config-driven data pipeline with Prefect orchestration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default config
  python run_pipeline.py

  # Use fast synthetic generation for 1M records
  python run_pipeline.py --source polars_synthetic --mode speed --scale 1000000

  # Process existing day04 data
  python run_pipeline.py --source file --path "../day04_pubsub/data/raw/*.jsonl"

  # Serve flow locally (blocks until stopped)
  python run_pipeline.py --serve

  # Serve with scheduled runs every 6 hours
  python run_pipeline.py --serve --cron "0 */6 * * *"
        """,
    )

    parser.add_argument(
        "--config",
        type=str,
        help="Path to pipeline.yaml (default: uses $PIPELINE_CONFIG)",
    )

    # Runtime overrides
    parser.add_argument(
        "--source",
        type=str,
        choices=["pubsub", "polars_synthetic", "file", "dlt"],
        help="Override data source type",
    )

    parser.add_argument(
        "--mode",
        type=str,
        choices=["chaos", "speed"],
        help="Override generation mode (chaos=realistic, speed=fast synthetic)",
    )

    parser.add_argument(
        "--scale",
        type=int,
        help="Override number of records to generate",
    )

    parser.add_argument(
        "--path",
        type=str,
        help="Override file path (for file source)",
    )

    # Prefect server options
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Start Prefect flow server (use with .serve())",
    )

    parser.add_argument(
        "--cron",
        type=str,
        help="Cron schedule for flow runs (requires --serve). Example: '0 */6 * * *'",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging()

    # Handle serve mode
    if args.serve:
        print("🚀 Starting Prefect flow server...")
        print("📊 View dashboard: http://127.0.0.1:4200")
        print("⏸️  Press Ctrl+C to stop")

        # Build serve parameters
        serve_params = {
            "name": "config-driven-pipeline-deployment",
        }

        if args.cron:
            serve_params["cron"] = args.cron
            print(f"📅 Scheduled runs: {args.cron}")

        # Serve the flow (blocks)
        run_pipeline.serve(**serve_params)

    else:
        # Direct execution mode
        print("▶️  Executing pipeline...")

        # Build overrides
        override_source = args.source
        override_mode = args.mode

        # For file source, we need to update config (simplified for now)
        if args.path:
            print(f"📁 Using file path: {args.path}")
            # TODO: Could inject this into config or pass as param

        # Run the flow
        result = run_pipeline(
            config_path=args.config,
            override_source=override_source,
            override_mode=override_mode,
        )

        print("\n✅ Pipeline completed successfully!")
        print(f"📊 Processed {result['input_records']:,} records")
        print(f"🎯 Strategies executed: {', '.join(result['strategies'].keys())}")


if __name__ == "__main__":
    main()
