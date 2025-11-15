"""
File ingestion source - reads existing JSONL files.
Supports glob patterns for batch processing.
"""

from glob import glob
from pathlib import Path

import polars as pl

from .base import DataSource, register_source


@register_source("file")
class FileSource(DataSource):
    """
    Ingest data from existing JSONL files.

    Supports glob patterns for reading multiple files.
    Validates schema matches expected format.
    """

    def generate(self) -> Path:
        """
        Read and validate existing JSONL file(s).

        Returns:
            Path to the input file (or combined output if multiple files)
        """
        file_path = self.config["source"].get("path")
        if not file_path:
            raise ValueError("File source requires 'path' in source config")

        self.logger.info("ingesting_file", pattern=file_path)

        # Expand glob pattern
        matching_files = glob(str(file_path))
        if not matching_files:
            raise FileNotFoundError(f"No files found matching pattern: {file_path}")

        self.logger.info("files_found", count=len(matching_files))

        # If single file, validate and return
        if len(matching_files) == 1:
            input_path = Path(matching_files[0])
            self._validate_schema(input_path)
            return input_path

        # Multiple files: combine into single JSONL
        return self._combine_files(matching_files)

    def _validate_schema(self, path: Path) -> None:
        """
        Validate that file has required fields.

        Args:
            path: Path to JSONL file
        """
        file_config = self.config.get("file", {})
        required_fields = file_config.get("schema", {}).get(
            "required_fields", ["message_id", "sequence_id", "partition_id", "created_at", "value"]
        )

        # Read first record to check schema
        df = pl.read_ndjson(path, n_rows=1)
        missing_fields = set(required_fields) - set(df.columns)

        if missing_fields:
            raise ValueError(f"File {path} missing required fields: {missing_fields}")

        self.logger.debug("schema_validated", path=str(path))

    def _combine_files(self, file_paths: list[str]) -> Path:
        """
        Combine multiple JSONL files into one.

        Args:
            file_paths: List of file paths to combine

        Returns:
            Path to combined output file
        """
        # Read and concatenate all files
        dfs = [pl.read_ndjson(f) for f in file_paths]
        combined = pl.concat(dfs)

        # Write to output
        raw_path = Path(self.config["storage"]["raw_data_path"])
        raw_path.mkdir(parents=True, exist_ok=True)

        output_path = raw_path / "combined_input.jsonl"
        combined.write_ndjson(output_path)

        self.logger.info(
            "files_combined",
            input_count=len(file_paths),
            output_path=str(output_path),
            total_records=len(combined),
        )

        return output_path
