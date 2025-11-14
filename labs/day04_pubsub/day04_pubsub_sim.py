#!/usr/bin/env python
"""
Day 4 – Pub/Sub Simulation with asyncio (v2 - Enhanced)

Enhanced version with additional fields (value, zipcode) for richer dedup experiments.

Run:
    uv run python labs/day04_pubsub/day04_pubsub_sim_v2.py --messages 100 --partitions 3
"""

import argparse
import asyncio
import json
import logging
import os
import random
import string
import time
from dataclasses import dataclass
from pathlib import Path

# Data paths from environment
RAW_DATA_PATH = Path(os.environ["RAW_DATA_PATH"])


# ------------------------
# Models & Helpers
# ------------------------


@dataclass
class Message:
    message_id: str
    sequence_id: int
    partition_id: int
    created_at: float  # epoch seconds
    value: int  # random business value
    zipcode: str  # random 5-digit zipcode


def random_id(length: int = 8) -> str:
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def random_zipcode() -> str:
    """Generate a random 5-digit zipcode."""
    return f"{random.randint(10000, 99999)}"


def setup_logging(quiet: bool = False) -> None:
    """Setup logging. If quiet=True, suppress per-message logs."""
    level = logging.WARNING if quiet else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",  # raw message only; we'll emit JSON
    )


# ------------------------
# Publisher
# ------------------------


async def publisher(  # noqa: PLR0913
    queue: asyncio.Queue,
    total_messages: int,
    partitions: int,
    base_delay: float = 0.01,
    jitter: float = 0.02,
    dup_prob: float = 0.1,
    slow_prob: float = 0.1,
) -> list[Message]:
    """
    Simulate a publisher that:
      - assigns a monotonically increasing sequence_id
      - randomly duplicates messages
      - randomly delays some messages (to create out-of-order arrival)
      - includes business data (value, zipcode)
    """
    produced: list[Message] = []

    for seq in range(1, total_messages + 1):
        # Random sleep with jitter
        delay = max(0.0, random.uniform(base_delay - jitter, base_delay + jitter))
        await asyncio.sleep(delay)

        partition_id = random.randint(0, partitions - 1)
        msg = Message(
            message_id=random_id(),
            sequence_id=seq,
            partition_id=partition_id,
            created_at=time.time(),
            value=random.randint(100, 10000),
            zipcode=random_zipcode(),
        )
        produced.append(msg)

        # Some messages get "slow path" treatment to skew ordering
        if random.random() < slow_prob:
            # schedule a later send
            asyncio.create_task(delayed_send(queue, msg, extra_delay=0.2))
        else:
            await queue.put(msg)

        # Occasionally produce a duplicate of a *previous* message
        if produced and random.random() < dup_prob:
            dup_msg = random.choice(produced)
            # Note: we re-send with same message_id & sequence_id
            await queue.put(dup_msg)

    # Signal completion
    await queue.put(None)  # type: ignore[arg-type]
    return produced


async def delayed_send(queue: asyncio.Queue, msg: Message, extra_delay: float) -> None:
    await asyncio.sleep(extra_delay)
    await queue.put(msg)


# ------------------------
# Subscriber
# ------------------------


async def subscriber(queue: asyncio.Queue, output_file: Path) -> list[dict]:
    """
    Consume messages from queue and log structured arrival info.
    Writes events to JSONL file and returns list of received-records for analysis.
    """
    records: list[dict] = []
    arrival_index = 0

    with open(output_file, "w") as f:
        while True:
            item: Message | None = await queue.get()
            if item is None:  # publisher completion sentinel
                queue.task_done()
                break

            arrival_index += 1
            arrival_ts = time.time()

            record = {
                "event": "message_received",
                "message_id": item.message_id,
                "sequence_id": item.sequence_id,
                "partition_id": item.partition_id,
                "created_at": item.created_at,
                "arrival_ts": arrival_ts,
                "arrival_index": arrival_index,
                "value": item.value,
                "zipcode": item.zipcode,
            }
            records.append(record)

            # Write to JSONL file
            f.write(json.dumps(record) + "\n")

            # Also log to stdout for real-time monitoring
            logging.info(json.dumps(record))
            queue.task_done()

    return records


# ------------------------
# Analysis
# ------------------------


def analyze(
    produced: list[Message],
    received: list[dict],
) -> None:
    """
    Simple offline analysis for:
      - duplicates
      - missing sequence IDs
      - ordering skew per partition
    """
    print("\n=== Analysis ===")

    produced_seq_ids = {m.sequence_id for m in produced}
    received_seq_ids = [r["sequence_id"] for r in received]

    # Missing messages (produced but never received)
    missing = sorted(produced_seq_ids - set(received_seq_ids))
    print(f"Total produced: {len(produced)}")
    print(f"Total received: {len(received)}")
    print(f"Missing sequence_ids: {missing if missing else 'None'}")

    # Duplicates
    seen = set()
    dupes = set()
    for seq in received_seq_ids:
        if seq in seen:
            dupes.add(seq)
        else:
            seen.add(seq)

    print(f"Duplicate sequence_ids: {sorted(dupes) if dupes else 'None'}")

    # Ordering skew per partition
    by_partition: dict[int, list[int]] = {}
    for r in received:
        by_partition.setdefault(r["partition_id"], []).append(r["sequence_id"])

    print("\nOrdering by partition:")
    for pid, seqs in by_partition.items():
        is_sorted = seqs == sorted(seqs)
        print(f"  partition {pid}: {seqs[:15]}{'...' if len(seqs) > 15 else ''}")
        if not is_sorted:
            print("    -> Out-of-order detected")

    print("=== End Analysis ===\n")


# ------------------------
# Entrypoint
# ------------------------


async def main(args: argparse.Namespace) -> None:
    setup_logging(quiet=args.quiet)
    queue: asyncio.Queue = asyncio.Queue(maxsize=args.buffer)

    # Generate timestamped output file
    timestamp = int(time.time())
    output_file = RAW_DATA_PATH / f"pubsub_sim_{timestamp}.jsonl"

    # Run publisher and subscriber concurrently
    subscriber_task = asyncio.create_task(subscriber(queue, output_file))
    produced = await publisher(
        queue=queue,
        total_messages=args.messages,
        partitions=args.partitions,
        base_delay=args.base_delay,
        jitter=args.jitter,
        dup_prob=args.dup_prob,
        slow_prob=args.slow_prob,
    )
    await queue.join()
    received = await subscriber_task

    analyze(produced, received)
    print(f"\nEvents saved to: {output_file}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Asyncio Pub/Sub Simulation (v2 - Enhanced)")
    parser.add_argument("--messages", type=int, default=100, help="Total messages to publish")
    parser.add_argument(
        "--scale",
        type=str,
        choices=["small", "medium", "large", "xlarge"],
        help="Preset scale: small=100, medium=10k, large=1M, xlarge=10M (overrides --messages)",
    )
    parser.add_argument("--partitions", type=int, default=3, help="Number of partitions")
    parser.add_argument("--buffer", type=int, default=100, help="Queue buffer size")
    parser.add_argument(
        "--base-delay", type=float, default=0.01, help="Base delay between publishes"
    )
    parser.add_argument(
        "--jitter", type=float, default=0.02, help="Random jitter around base delay"
    )
    parser.add_argument(
        "--dup-prob", type=float, default=0.1, help="Probability of emitting a duplicate"
    )
    parser.add_argument(
        "--slow-prob",
        type=float,
        default=0.1,
        help="Probability of sending a slow (out-of-order) message",
    )
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-message logging (recommended for large runs)",
    )

    args = parser.parse_args()

    # Apply scale preset if provided
    if args.scale:
        scale_map = {"small": 100, "medium": 10_000, "large": 1_000_000, "xlarge": 10_000_000}
        args.messages = scale_map[args.scale]
        print(f"Using scale preset '{args.scale}': {args.messages:,} messages")

    # Set random seed for reproducibility
    if args.seed:
        random.seed(args.seed)
        print(f"Random seed set to: {args.seed}")

    return args


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
