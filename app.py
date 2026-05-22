"""
Real-Time Sensor Data Processing System
EN5500 - Computer Systems Assignment 1

This module implements an async streaming sensor data processor
with background tasks for continuous data handling.
"""

# Standard imports
import asyncio
import random
import time
from typing import Optional, Union, List
from dataclasses import dataclass, asdict
from collections import deque
from contextlib import asynccontextmanager

# Third party imports
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel


@dataclass
class Statistics:
    """Container for statistical results of a data block."""

    # pylint: disable=too-many-instance-attributes

    block_number: int
    max_value: Optional[float]
    min_value: Optional[float]
    average: Optional[float]
    std_deviation: Optional[float]
    missing_samples: int
    corrupted_samples: int
    valid_samples: int
    processing_time_ms: float
    timestamp: float


class BlockSizeChangeError(Exception):
    """Custom exception for block size change errors."""


class SensorDataGenerator:
    # pylint: disable=too-few-public-methods
    """Asynchronous sensor data generator."""

    def __init__(self, error_rate: float = 0.05, missing_rate: float = 0.02):
        self.error_rate = error_rate
        self.missing_rate = missing_rate

    async def generate_sample(self) -> Optional[Union[int, str]]:
        """Generate a single sensor sample asynchronously."""
        await asyncio.sleep(0)
        if random.random() < self.missing_rate:
            return None
        if random.random() < self.error_rate:
            return random.choice(["ERROR", -1, 101, "INVALID"])
        return random.randint(0, 100)


class StatisticsCalculator:
    # pylint: disable=too-few-public-methods
    """
    Statistics calculator for sensor data blocks.

    PART (a) — Statistical Parameters:
        Computes max, min, mean, and *sample* standard deviation (Bessel's
        correction, dividing by N-1) for every block of valid readings.
        Population std dev (÷N) underestimates variability when the block is
        treated as a sample drawn from a larger sensor population, so N-1 is
        the correct convention here.

    PART (c) — Data Quality Classification:
        • Missing  : sample is None  → excluded from stats, counted separately.
        • Corrupted: value is non-numeric OR outside the valid sensor range
                     [0, 100]  → excluded from stats, counted separately.
        Separating the two categories lets operators distinguish between a
        dead sensor (many missing) and a noisy/faulty one (many corrupted).
        Effect on accuracy: missing/corrupted samples reduce the effective N,
        potentially biasing the mean and inflating or deflating std deviation
        compared to a full clean block.
    """

    @staticmethod
    async def calculate(block_data: List) -> Statistics:
        """
        Calculate statistics for a block of sensor data.

        Uses sample standard deviation (Bessel's correction, denominator N-1)
        which is the statistically correct choice when the block represents a
        sample from a continuous sensor stream rather than the entire
        population.  Returns None for std_deviation when fewer than 2 valid
        samples exist (std dev is undefined for N < 2).
        """
        valid_samples = []
        missing_count = 0
        corrupted_count = 0

        for sample in block_data:
            if sample is None:
                missing_count += 1
            elif isinstance(sample, (int, float)) and 0 <= sample <= 100:
                valid_samples.append(sample)
            else:
                corrupted_count += 1

        if valid_samples:
            n = len(valid_samples)
            max_value = max(valid_samples)
            min_value = min(valid_samples)
            average = sum(valid_samples) / n

            # Sample std deviation (Bessel's correction: divide by N-1).
            # Guard: std dev is undefined for a single sample → return 0.0
            # so the caller always receives a numeric value when data exists.
            if n > 1:
                variance = sum((x - average) ** 2 for x in valid_samples) / (n - 1)
                std_deviation = variance**0.5
            else:
                std_deviation = 0.0
        else:
            max_value = min_value = average = std_deviation = None

        return Statistics(
            block_number=0,
            max_value=max_value,
            min_value=min_value,
            average=average,
            std_deviation=std_deviation,
            missing_samples=missing_count,
            corrupted_samples=corrupted_count,
            valid_samples=len(valid_samples),
            processing_time_ms=0,  # Filled in by the caller after timing
            timestamp=time.time(),
        )


def _print_block_summary(stats: "Statistics", expected_size: int) -> None:
    """
    PART (a) — Display computed results after processing each block.

    Prints a formatted table showing all required statistical parameters:
    max, min, mean, sample std deviation, missing count, and corrupted count.
    An incomplete-block warning is shown when valid+missing+corrupted is less
    than the configured block size (caused by a timeout flush).
    """
    w = 50
    total_received = (
        stats.valid_samples + stats.missing_samples + stats.corrupted_samples
    )
    incomplete = total_received < expected_size
    tag = " [TIMEOUT / INCOMPLETE]" if incomplete else ""

    def _fmt(val, decimals=4):
        return f"{val:.{decimals}f}" if val is not None else "N/A"

    print(f"\n{'═' * w}")
    print(f"  Block #{stats.block_number:>4}{tag}")
    print(f"{'─' * w}")
    print(f"  {'Metric':<28} {'Value':>16}")
    print(f"{'─' * w}")
    print(f"  {'Maximum value':<28} {_fmt(stats.max_value, 0):>16}")
    print(f"  {'Minimum value':<28} {_fmt(stats.min_value, 0):>16}")
    print(f"  {'Average (mean)':<28} {_fmt(stats.average):>16}")
    print(f"  {'Std deviation (sample)':<28} {_fmt(stats.std_deviation):>16}")
    print(f"{'─' * w}")
    print(f"  {'Valid samples':<28} {stats.valid_samples:>14}/{expected_size}")
    print(f"  {'Missing samples':<28} {stats.missing_samples:>16}")
    print(f"  {'Corrupted samples':<28} {stats.corrupted_samples:>16}")
    print(f"{'─' * w}")
    print(f"  {'Processing time':<28} {stats.processing_time_ms:>13.3f}ms")
    print(f"{'═' * w}")


class AsyncSensorDataProcessor:
    # pylint: disable=too-many-instance-attributes
    """
    Main processor for real-time sensor data streaming.

    PART (b) — Performance Under High Data Rate:
        The producer and consumer run as independent asyncio tasks (effectively
        a double-buffer / producer-consumer pattern).  The producer deposits
        samples into an asyncio.Queue (bounded to max_queue_size).  If the
        queue fills faster than the consumer can drain it, new samples are
        dropped and counted in queue_overflow_count — a deliberate,
        observable degradation instead of an uncontrolled crash.
        processing_time_ms recorded per block makes latency visible.

    PART (c) — Delayed Data / Timeout Mechanism:
        A dedicated _timeout_watchdog task monitors the current block.  If no
        new sample has been added for block_timeout_seconds, it force-flushes
        whatever partial data has accumulated.  This prevents the system from
        stalling indefinitely when the sensor goes silent mid-block (delayed
        or dropped tail samples).  The flushed Statistics object is tagged
        with the actual received sample count so downstream consumers can
        detect the incomplete block.

    PART (d) — Design Improvements:
        1. Async Queue with overflow detection (see above) — decouples
           acquisition from computation, absorbs bursts, prevents OOM.
        2. Timeout-based partial-block flushing (see above) — ensures
           bounded latency even when data arrives irregularly.
    """

    # pylint: disable=broad-exception-caught

    def __init__(
        self,
        block_size: int = 100,
        max_queue_size: int = 1000,
        block_timeout_seconds: float = 5.0,
    ):
        # Configuration
        self._block_size = block_size
        self.max_queue_size = max_queue_size
        self.min_block_size = 1
        self.max_block_size = 10000

        # PART (c): Timeout — flush a partial block after this many seconds of
        # inactivity.  Prevents the system stalling when delayed sensor data
        # leaves a block permanently incomplete.
        self.block_timeout_seconds = block_timeout_seconds
        self._last_sample_time: float = time.time()

        # Processing state
        self.current_block = []
        self.block_count = 0

        # Components
        self.generator = SensorDataGenerator()
        self.calculator = StatisticsCalculator()

        # Async synchronization
        self.lock = asyncio.Lock()
        self.data_queue = asyncio.Queue(maxsize=max_queue_size)
        self.block_size_change_event = asyncio.Event()
        self.block_size_change_pending = False
        self.pending_block_size = None

        # Storage
        self.completed_blocks = deque(maxlen=10)
        self.current_stats = None

        # Control flags
        self.is_running = True
        self.queue_overflow_count = 0
        self.timeout_flush_count = 0  # How many blocks were flushed by timeout

        # Background tasks
        self.producer_task = None
        self.consumer_task = None
        self.timeout_task = None  # PART (c): watchdog for delayed-data timeout

    @property
    def block_size(self) -> int:
        """Get current block size."""
        return self._block_size

    async def set_block_size(self, new_size: int, force: bool = False) -> dict:
        """
        Change the block size at runtime.

        Args:
            new_size: New block size (must be between min_block_size and max_block_size)
            force: If True, immediately process current block regardless of size

        Returns:
            Dictionary with change status and details
        """
        if not self.min_block_size <= new_size <= self.max_block_size:
            raise ValueError(
                f"Block size must be between {self.min_block_size} and {self.max_block_size}"
            )

        async with self.lock:
            old_size = self._block_size

            # If force=True, process the current block immediately
            if force and self.current_block:
                await self._process_current_block()

            # Check if current block would exceed new size
            current_size = len(self.current_block)
            if current_size >= new_size:
                # Need to split current block
                return await self._handle_block_size_decrease(new_size)
            if current_size > 0:
                # Current block fits within new size, will continue accumulating
                self._block_size = new_size
                return {
                    "status": "success",
                    "old_size": old_size,
                    "new_size": new_size,
                    "current_block_size": current_size,
                    "action": "size_increased",
                    "message": f"Block size changed from {old_size} to {new_size}. "
                    f"Current block ({current_size} samples) will continue accumulating.",
                }
            # Empty block, simple change
            self._block_size = new_size
            return {
                "status": "success",
                "old_size": old_size,
                "new_size": new_size,
                "current_block_size": 0,
                "action": "simple_change",
                "message": f"Block size changed from {old_size} to {new_size}",
            }

    async def _handle_block_size_decrease(self, new_size: int) -> dict:
        """
        Handle decreasing block size when current block has more samples than new size.
        """
        old_size = self._block_size

        # Process samples in batches of new_size
        blocks_created = 0
        remaining_samples = self.current_block.copy()
        self.current_block = []

        while len(remaining_samples) >= new_size:
            # Take a chunk of new_size samples
            chunk = remaining_samples[:new_size]
            remaining_samples = remaining_samples[new_size:]

            # Process this chunk as a block
            start_time = time.time()
            stats = await self.calculator.calculate(chunk)
            stats.block_number = self.block_count + 1
            stats.processing_time_ms = (time.time() - start_time) * 1000

            self.block_count += 1
            self.current_stats = stats
            self.completed_blocks.append(stats)
            blocks_created += 1

            _print_block_summary(stats, expected_size=new_size)

        # Keep remaining samples for next block
        self.current_block = remaining_samples
        self._block_size = new_size

        return {
            "status": "success",
            "old_size": old_size,
            "new_size": new_size,
            "blocks_created": blocks_created,
            "remaining_samples": len(self.current_block),
            "action": "size_decreased_with_split",
            "message": f"Block size decreased from {old_size} to {new_size}. "
            f"Split current block into {blocks_created} blocks of size {new_size}. "
            f"{len(self.current_block)} samples remain.",
        }

    async def _process_current_block(self):
        """
        Force process the current block regardless of size.

        Measures wall-clock processing time (part of latency tracking for
        Part b) and prints a formatted summary to the console so results
        are immediately visible after each block — satisfying Part (a) req 5.
        """
        if not self.current_block:
            return

        start_time = time.time()
        stats = await self.calculator.calculate(self.current_block)
        stats.block_number = self.block_count + 1
        stats.processing_time_ms = (time.time() - start_time) * 1000

        self.block_count += 1
        self.current_stats = stats
        self.completed_blocks.append(stats)

        # ── PART (a): Formatted console output after each block ──────────
        _print_block_summary(stats, expected_size=self._block_size)

        self.current_block = []

    async def _producer(self):
        """Background task for continuous sensor data generation."""
        print("[Producer] Starting sensor data generation...")
        sample_count = 0

        while self.is_running:
            try:
                sample = await self.generator.generate_sample()
                sample_count += 1

                try:
                    await asyncio.wait_for(self.data_queue.put(sample), timeout=0.1)
                except asyncio.TimeoutError:
                    self.queue_overflow_count += 1
                    print(f"[WARNING] Queue overflow! Dropped sample #{sample_count}")
                    continue

                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                print(f"[Producer Error] {exc}")
                await asyncio.sleep(0.1)

    async def _consumer(self):
        """
        Background task for processing data into blocks.

        PART (b): Runs concurrently with the producer via asyncio, so data
        ingestion is never blocked by statistics computation.
        PART (c): Records the timestamp of each ingested sample so the
        timeout watchdog can detect inactivity and flush partial blocks.
        """
        print("[Consumer] Starting data processing...")

        while self.is_running:
            try:
                sample = await self.data_queue.get()

                async with self.lock:
                    self.current_block.append(sample)
                    self._last_sample_time = (
                        time.time()
                    )  # PART (c): update activity clock

                    # Check if we've reached the block size
                    if len(self.current_block) >= self._block_size:
                        await self._process_current_block()

                self.data_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                print(f"[Consumer Error] {exc}")
                await asyncio.sleep(0.1)

    async def _timeout_watchdog(self):
        """
        PART (c) — Delayed Data Timeout Mechanism:

        Polls every second.  If the current block is non-empty AND no new
        sample has arrived for block_timeout_seconds, it force-flushes the
        partial block.  This prevents indefinite stalling when the sensor
        goes silent mid-block (e.g. network delay, sensor fault).

        Effect on accuracy: a timed-out block will have fewer than
        block_size samples.  The valid_samples field in Statistics records
        the actual count, allowing downstream consumers to discount the
        result appropriately.
        """
        print(
            f"[Watchdog] Timeout monitor active ({self.block_timeout_seconds}s threshold)"
        )

        while self.is_running:
            try:
                await asyncio.sleep(1.0)

                async with self.lock:
                    if not self.current_block:
                        continue

                    idle_seconds = time.time() - self._last_sample_time
                    if idle_seconds >= self.block_timeout_seconds:
                        samples_held = len(self.current_block)
                        print(
                            f"\n[Watchdog] ⚠ No data for {idle_seconds:.1f}s — "
                            f"force-flushing partial block ({samples_held} samples)"
                        )
                        await self._process_current_block()
                        self.timeout_flush_count += 1
                        # Reset clock so watchdog doesn't fire again immediately
                        self._last_sample_time = time.time()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                print(f"[Watchdog Error] {exc}")
                await asyncio.sleep(1.0)

    async def get_status(self) -> dict:
        """Get current processing status."""
        async with self.lock:
            return {
                "is_running": self.is_running,
                "block_size": self._block_size,
                "block_timeout_seconds": self.block_timeout_seconds,
                "min_block_size": self.min_block_size,
                "max_block_size": self.max_block_size,
                "current_block_size": len(self.current_block),
                "blocks_completed": self.block_count,
                "queue_size": self.data_queue.qsize(),
                "queue_max_size": self.max_queue_size,
                "queue_overflow_count": self.queue_overflow_count,
                "timeout_flush_count": self.timeout_flush_count,
                "remaining_needed": self._block_size - len(self.current_block),
                "latest_block": (
                    asdict(self.current_stats) if self.current_stats else None
                ),
            }

    async def start_processing(self):
        """Start the background producer, consumer, and timeout watchdog tasks."""
        self.is_running = True
        self._last_sample_time = time.time()
        self.producer_task = asyncio.create_task(self._producer())
        self.consumer_task = asyncio.create_task(self._consumer())
        self.timeout_task = asyncio.create_task(self._timeout_watchdog())  # PART (c)
        print("[System] Background processing started")

    async def stop_processing(self):
        """Stop all background processing tasks gracefully."""
        self.is_running = False
        tasks = [
            t for t in (self.producer_task, self.consumer_task, self.timeout_task) if t
        ]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        print("[System] Background processing stopped")

    async def reset(self):
        """Reset the entire processing system."""
        async with self.lock:
            self.current_block = []
            self.block_count = 0
            self.completed_blocks.clear()
            self.current_stats = None
            self.queue_overflow_count = 0
            self.timeout_flush_count = 0
            self._last_sample_time = time.time()

            while not self.data_queue.empty():
                try:
                    self.data_queue.get_nowait()
                    self.data_queue.task_done()
                except asyncio.QueueEmpty:
                    break

    async def force_process(self) -> dict:
        """
        Public API for externally triggering an immediate flush of the current
        partial block.  Routes to the internal _process_current_block so that
        API endpoint code never touches a protected member directly (W0212).
        Returns a summary dict suitable for an HTTP response.
        """
        async with self.lock:
            if not self.current_block:
                return {
                    "message": "No current block to process",
                    "samples_processed": 0,
                }

            samples_before = len(self.current_block)
            await self._process_current_block()
            return {
                "message": f"Force processed block of {samples_before} samples",
                "samples_processed": samples_before,
                "new_block_size": self.block_size,
                "current_block_size": len(self.current_block),
            }


# ============ Pydantic Models for API ============


class BlockSizeChangeRequest(BaseModel):
    """Request model for changing block size."""

    new_size: int
    force_process: bool = False


class BlockSizeChangeResponse(BaseModel):
    """Response model for block size change."""

    status: str
    old_size: int
    new_size: int
    current_block_size: int
    action: str
    message: str
    blocks_created: Optional[int] = None
    remaining_samples: Optional[int] = None


# ============ LIFESPAN EVENTS (Modern approach) ============


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    """
    Manage the application lifespan.
    This replaces the deprecated @app.on_event() decorators.

    Code before 'yield' runs at startup.
    Code after 'yield' runs at shutdown.
    """
    # STARTUP: Initialize processor and start background tasks
    print("[Lifespan] Application starting up...")
    processor = AsyncSensorDataProcessor(
        block_size=100,
        max_queue_size=1000,
        block_timeout_seconds=5.0,  # PART (c): flush partial block after 5 s of silence
    )
    await processor.start_processing()

    # Store processor in app state for access in endpoints
    fastapi_app.state.processor = processor

    yield  # The application runs here

    # SHUTDOWN: Clean up resources
    print("[Lifespan] Application shutting down...")
    await processor.stop_processing()


# Create FastAPI app with lifespan manager
app = FastAPI(
    title="Real-Time Sensor Data Processing System",
    description="EN5500 - Computer Systems Assignment 1 - With Runtime Configurable Block Size",
    version="2.1",
    lifespan=lifespan,  # Use modern lifespan instead of on_event
)


# ============ API Endpoints ============


@app.get("/")
async def root():
    """API root endpoint with documentation."""
    return {
        "assignment": "EN5500 - Computer Systems Assignment 1",
        "title": "Real-Time Sensor Data Processing System",
        "architecture": "Async streaming with background tasks",
        "features": {
            "runtime_block_size_configuration": True,
            "dynamic_blocks": True,
            "graceful_block_splitting": True,
        },
        "endpoints": {
            "/status": "System status and current block progress",
            "/stats/latest": "Most recent completed block statistics",
            "/stats/all": "All completed blocks",
            "/stream": "Server-Sent Events stream of completed blocks",
            "/control/start": "Start processing (if stopped)",
            "/control/stop": "Stop processing",
            "/control/reset": "Reset all data",
            "/config/block-size": "GET: Get current block size, POST: Change block size",
            "/config/block-size/validate": "Validate a block size without changing",
        },
    }


@app.get("/status")
async def get_status():
    """Get current system processing status."""
    return await app.state.processor.get_status()


@app.get("/stats/latest")
async def get_latest_stats():
    """Get the most recently completed block statistics."""
    status = await app.state.processor.get_status()
    if status["latest_block"]:
        return status["latest_block"]
    return {
        "message": "No blocks completed yet",
        "current_progress": status["current_block_size"],
        "target_block_size": status["block_size"],
    }


@app.get("/stats/all")
async def get_all_stats():
    """Get all completed blocks."""
    async with app.state.processor.lock:
        return {
            "total_blocks": app.state.processor.block_count,
            "blocks": [asdict(stats) for stats in app.state.processor.completed_blocks],
        }


@app.get("/stream")
async def stream_blocks():
    """Server-Sent Events endpoint for real-time block updates."""

    async def event_generator():
        last_block_count = 0

        while True:
            status = await app.state.processor.get_status()
            current_count = status["blocks_completed"]

            if current_count > last_block_count:
                async with app.state.processor.lock:
                    if app.state.processor.completed_blocks:
                        latest = app.state.processor.completed_blocks[-1]
                        # Include block size info in the stream
                        event_data = asdict(latest)
                        event_data["block_size_when_processed"] = status["block_size"]
                        yield f"data: {event_data}\n\n"
                last_block_count = current_count

            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


# ============ Block Size Configuration Endpoints ============


@app.get("/config/block-size")
async def get_block_size():
    """Get the current block size configuration."""
    processor = app.state.processor
    async with processor.lock:
        return {
            "current_block_size": processor.block_size,
            "min_allowed": processor.min_block_size,
            "max_allowed": processor.max_block_size,
            "current_block_samples": len(processor.current_block),
            "remaining_needed": processor.block_size - len(processor.current_block),
            "can_change": True,
            "note": "Block size can be changed at any time. "
            "If decreasing, current block will be split into smaller blocks.",
        }


@app.post("/config/block-size")
async def change_block_size(request: BlockSizeChangeRequest):
    """
    Change the block size at runtime.

    - If increasing: Current block continues accumulating up to new size
    - If decreasing: Current block is split into multiple blocks of the new size
    - Use force_process=True to process current block immediately before changing
    """
    processor = app.state.processor

    try:
        result = await processor.set_block_size(
            request.new_size, force=request.force_process
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/config/block-size/validate")
async def validate_block_size(new_size: int):
    """Validate if a block size is acceptable without changing it."""
    processor = app.state.processor

    async with processor.lock:
        is_valid = processor.min_block_size <= new_size <= processor.max_block_size

        return {
            "requested_size": new_size,
            "is_valid": is_valid,
            "min_allowed": processor.min_block_size,
            "max_allowed": processor.max_block_size,
            "current_size": processor.block_size,
            "notes": {
                "can_increase": new_size > processor.block_size if is_valid else False,
                "can_decrease": new_size < processor.block_size if is_valid else False,
                "will_split_block": (
                    (
                        new_size < processor.block_size
                        and len(processor.current_block) > new_size
                    )
                    if is_valid
                    else False
                ),
            },
        }


@app.post("/config/block-size/force-process")
async def force_process_current_block():
    """Force process the current block regardless of its size."""
    return await app.state.processor.force_process()


# ============ Control Endpoints ============


@app.post("/control/start")
async def start_processing():
    """Start or resume background processing."""
    if not app.state.processor.is_running:
        await app.state.processor.start_processing()
        return {"message": "Processing started"}
    return {"message": "Processing already running"}


@app.post("/control/stop")
async def stop_processing():
    """Stop background processing."""
    if app.state.processor.is_running:
        await app.state.processor.stop_processing()
        return {"message": "Processing stopped"}
    return {"message": "Processing already stopped"}


@app.post("/control/reset")
async def reset_system():
    """Reset the entire processing system."""
    await app.state.processor.reset()
    return {"message": "System reset successfully"}


if __name__ == "__main__":
    import uvicorn

    print("\n" + "=" * 70)
    print("REAL-TIME SENSOR DATA PROCESSING SYSTEM")
    print("EN5500 - Computer Systems Assignment 1")
    print("=" * 70)
    print("Architecture : Async streaming with background tasks")
    print("Std deviation: Sample (Bessel's correction, ÷N-1)")
    print("Block timeout: 5.0 s  → partial blocks flushed on sensor silence")
    print("Block size   : Runtime configurable via POST /config/block-size")
    print("\nAPI Endpoints:")
    print("  - GET  /                               - API Documentation")
    print(
        "  - GET  /status                         - System status (incl. timeout_flush_count)"
    )
    print("  - GET  /stats/latest                   - Latest block stats")
    print("  - GET  /stats/all                      - All completed blocks")
    print("  - GET  /stream                         - Real-time SSE stream")
    print("  - GET  /config/block-size              - Get current block size")
    print("  - POST /config/block-size              - Change block size at runtime")
    print("  - POST /config/block-size/validate     - Validate block size")
    print("  - POST /config/block-size/force-process- Force process current block")
    print("  - POST /control/start                  - Start processing")
    print("  - POST /control/stop                   - Stop processing")
    print("  - POST /control/reset                  - Reset all data")
    print("=" * 70)
    print("\nBLOCK SIZE CONFIGURATION EXAMPLES:")
    print("  # Get current size")
    print("  curl http://localhost:8080/config/block-size")
    print("\n  # Change to 50 samples per block")
    print("  curl -X POST http://localhost:8080/config/block-size \\")
    print("       -H 'Content-Type: application/json' \\")
    print('       -d \'{"new_size": 50, "force_process": false}\'')
    print("\n  # Force process current block before changing to 200")
    print("  curl -X POST http://localhost:8080/config/block-size \\")
    print("       -H 'Content-Type: application/json' \\")
    print('       -d \'{"new_size": 200, "force_process": true}\'')
    print("=" * 70 + "\n")

    uvicorn.run(app, host="0.0.0.0", port=8080)
