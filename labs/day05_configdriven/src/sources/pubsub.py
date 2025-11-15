"""
Pub/Sub simulator source - refactored from day04.
Realistic asyncio-based message simulation with chaos engineering.
"""

import asyncio
import json
import random
import string
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .base import DataSource, register_source


@dataclass
class Message:
    """Message data structure matching day04 schema"""

    message_id: str
    sequence_id: int
    partition_id: int
    created_at: float  # epoch seconds
    value: int  # business value
    zipcode: str  # 5-digit zipcode


def random_id(length: int = 8) -> str:
    """Generate random alphanumeric ID"""
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(length))


@register_source("pubsub")
class PubSubSource(DataSource):
    """
    Asyncio-based pub/sub simulator with realistic chaos:
    - Duplicates: random previous messages re-sent
    - Out-of-order: delayed delivery on slow path
    - Configurable delays and jitter
    """

    def generate(self) -> Path:
        """
        Run asyncio pub/sub simulation.

        Returns:
            Path to generated JSONL file
        """
        scale = self.config["generation"]["scale"]
        seed = self.config["generation"]["seed"]
        chaos_config = self.config["generation"]["chaos"]
        source_config = self.config.get("pubsub", {})

        if seed is not None:
            random.seed(seed)

        # Sample chaos parameters if random mode
        if chaos_config["mode"] == "random":
            sampled_chaos = self._sample_chaos(chaos_config)
        else:
            sampled_chaos = self._fixed_chaos(chaos_config)

        self.logger.info(
            "starting_pubsub_simulation",
            scale=scale,
            dup_prob=sampled_chaos["dup_prob"],
            slow_prob=sampled_chaos["slow_prob"],
            seed=seed,
        )

        # Run asyncio simulation
        output_path = self._get_output_path()
        asyncio.run(
            self._run_simulation(
                total_messages=scale,
                output_file=output_path,
                partitions=source_config.get("partitions", 3),
                buffer_size=source_config.get("buffer_size", 100),
                message_config=source_config.get("message", {}),
                chaos=sampled_chaos,
                log_interval=source_config.get("log_interval", 10000),
            )
        )

        self.validate_output(output_path)

        self.logger.info(
            "pubsub_simulation_completed",
            path=str(output_path),
            size_mb=round(output_path.stat().st_size / 1024 / 1024, 2),
        )

        return output_path

    async def _run_simulation(  # noqa: PLR0913
        self,
        total_messages: int,
        output_file: Path,
        partitions: int,
        buffer_size: int,
        message_config: dict[str, Any],
        chaos: dict[str, float],
        log_interval: int,
    ) -> None:
        """Run the full pub/sub simulation"""
        queue: asyncio.Queue = asyncio.Queue(maxsize=buffer_size)

        # Start subscriber task
        subscriber_task = asyncio.create_task(
            self._subscriber(queue, output_file, total_messages, log_interval)
        )

        # Run publisher
        await self._publisher(
            queue=queue,
            total_messages=total_messages,
            partitions=partitions,
            message_config=message_config,
            base_delay=chaos["base_delay"],
            jitter=chaos["jitter"],
            dup_prob=chaos["dup_prob"],
            slow_prob=chaos["slow_prob"],
        )

        # Wait for all messages to be consumed
        await queue.join()
        await subscriber_task

    async def _publisher(  # noqa: PLR0913
        self,
        queue: asyncio.Queue,
        total_messages: int,
        partitions: int,
        message_config: dict[str, Any],
        base_delay: float,
        jitter: float,
        dup_prob: float,
        slow_prob: float,
    ) -> None:
        """
        Simulate message publishing with chaos:
        - Random duplicates of previous messages
        - Random slow-path delays (out-of-order)
        """
        produced: list[Message] = []

        id_length = message_config.get("id_length", 8)
        value_range = message_config.get("value_range", [100, 10000])

        for seq in range(1, total_messages + 1):
            # Random sleep with jitter
            delay = max(0.0, random.uniform(base_delay - jitter, base_delay + jitter))
            await asyncio.sleep(delay)

            partition_id = random.randint(0, partitions - 1)
            msg = Message(
                message_id=random_id(id_length),
                sequence_id=seq,
                partition_id=partition_id,
                created_at=time.time(),
                value=random.randint(value_range[0], value_range[1]),
                zipcode=str(random.randint(10000, 99999)),
            )
            produced.append(msg)

            # Some messages get slow-path treatment (out-of-order)
            if random.random() < slow_prob:
                asyncio.create_task(self._delayed_send(queue, msg, extra_delay=0.2))
            else:
                await queue.put(msg)

            # Occasionally duplicate a previous message
            if produced and random.random() < dup_prob:
                dup_msg = random.choice(produced)
                await queue.put(dup_msg)

        # Signal completion
        await queue.put(None)

    async def _delayed_send(self, queue: asyncio.Queue, msg: Message, extra_delay: float) -> None:
        """Send message after delay (simulates slow path)"""
        await asyncio.sleep(extra_delay)
        await queue.put(msg)

    async def _subscriber(
        self, queue: asyncio.Queue, output_file: Path, total_messages: int, log_interval: int
    ) -> None:
        """
        Consume messages and write to JSONL.
        Enriches with arrival metadata.
        """
        arrival_index = 0
        start_time = time.time()

        with open(output_file, "w") as f:
            while True:
                item: Message | None = await queue.get()
                if item is None:  # Completion sentinel
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

                f.write(json.dumps(record) + "\n")
                queue.task_done()

                # Log progress at intervals
                if arrival_index % log_interval == 0:
                    elapsed = time.time() - start_time
                    rate = arrival_index / elapsed if elapsed > 0 else 0
                    pct_complete = (
                        (arrival_index / total_messages) * 100 if total_messages > 0 else 0
                    )
                    eta_seconds = (total_messages - arrival_index) / rate if rate > 0 else 0

                    self.logger.info(
                        "pubsub_progress",
                        processed=arrival_index,
                        total=total_messages,
                        percent=round(pct_complete, 1),
                        rate_per_sec=round(rate, 1),
                        elapsed_sec=round(elapsed, 1),
                        eta_sec=round(eta_seconds, 1),
                    )

    def _sample_chaos(self, chaos_config: dict[str, Any]) -> dict[str, float]:
        """Sample chaos parameters from ranges"""
        return {
            "dup_prob": self._sample_value(chaos_config["dup_prob"]),
            "slow_prob": self._sample_value(chaos_config["slow_prob"]),
            "base_delay": self._sample_value(chaos_config["base_delay"]),
            "jitter": self._sample_value(chaos_config["jitter"]),
        }

    def _fixed_chaos(self, chaos_config: dict[str, Any]) -> dict[str, float]:
        """Use fixed chaos parameters"""
        return {
            "dup_prob": self._get_value(chaos_config["dup_prob"]),
            "slow_prob": self._get_value(chaos_config["slow_prob"]),
            "base_delay": self._get_value(chaos_config["base_delay"]),
            "jitter": self._get_value(chaos_config["jitter"]),
        }

    @staticmethod
    def _get_value(val):
        """Extract value from float or tuple"""
        return val if isinstance(val, int | float) else val[0]

    @staticmethod
    def _sample_value(val):
        """Sample from range or return fixed value"""
        if isinstance(val, list | tuple):
            return random.uniform(val[0], val[1])
        return val

    def _get_output_path(self) -> Path:
        """Construct output file path"""
        raw_path = Path(self.config["storage"]["raw_data_path"])
        raw_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"pubsub_sim_{timestamp}.jsonl"

        return raw_path / filename
