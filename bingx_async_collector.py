import json
import gzip
import io
import asyncio
import websockets
import csv
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bingx_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

URL = "wss://open-api-swap.bingx.com/swap-market"

@dataclass(frozen=True, slots=True)
class Candle:
    """Immutable candle data structure with slots for memory efficiency"""
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: int
    
    @property
    def datetime(self) -> datetime:
        """Convert timestamp to datetime object"""
        return datetime.utcfromtimestamp(self.timestamp // 1000)
    
    @property
    def formatted_time(self) -> str:
        """Get formatted time string"""
        return self.datetime.strftime('%Y-%m-%d %H:%M:%S')
    
    def __str__(self) -> str:
        return (f"[{self.formatted_time}] "
                f"O:{self.open} H:{self.high} L:{self.low} "
                f"C:{self.close} V:{self.volume}")

@dataclass(slots=True)
class SubscriptionChannel:
    """WebSocket subscription channel configuration"""
    id: str
    req_type: str
    data_type: str
    
    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for JSON serialization"""
        return {
            "id": self.id,
            "reqType": self.req_type,
            "dataType": self.data_type
        }

@dataclass(slots=True)
class ConnectionConfig:
    """WebSocket connection configuration"""
    url: str
    max_retries: Optional[int] = None
    initial_retry_delay: float = 1.0
    max_retry_delay: float = 30.0
    backoff_multiplier: float = 2.0
    
    def get_retry_delay(self, attempt: int) -> float:
        """Calculate retry delay with exponential backoff"""
        delay = self.initial_retry_delay * (self.backoff_multiplier ** attempt)
        return min(delay, self.max_retry_delay)

@dataclass(slots=True)
class CollectorStats:
    """Statistics for the collector"""
    candles_received: int = 0
    candles_processed: int = 0
    connection_attempts: int = 0
    successful_connections: int = 0
    errors_count: int = 0
    start_time: Optional[datetime] = None
    
    def __post_init__(self):
        if self.start_time is None:
            self.start_time = datetime.utcnow()
    
    @property
    def uptime_seconds(self) -> float:
        """Get uptime in seconds"""
        if self.start_time:
            return (datetime.utcnow() - self.start_time).total_seconds()
        return 0.0

class BingXProducerConsumer:
    def __init__(self, symbol: str = "BTC-USDT", interval: str = "3m", queue_size: int = 100):
        self._symbol = symbol
        self._interval = interval
        
        # Producer-Consumer queues
        self._raw_queue: asyncio.Queue[Candle] = asyncio.Queue(maxsize=queue_size)
        self._processed_queue: asyncio.Queue[Candle] = asyncio.Queue(maxsize=queue_size)
        
        # Control
        self._is_running = False
        self._producer_task: Optional[asyncio.Task] = None
        self._consumer_task: Optional[asyncio.Task] = None
        
        # Data and stats
        self._candles: List[Candle] = []
        self._stats = CollectorStats()
        
        # CSV file configuration
        self._csv_filename = f"{symbol}_{interval}_candles.csv"
        self._csv_headers = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        
        # Track last saved candle timestamp to avoid duplicates
        self._last_saved_timestamp: Optional[int] = None
        
        # Configuration
        self._config = ConnectionConfig(url=URL)
        self._channel = SubscriptionChannel(
            id="e745cd6d-d0f6-4a70-8d5a-043e4c741b40",
            req_type="sub",
            data_type=f"{symbol}@kline_{interval}"
        )

    def _init_csv_file(self) -> None:
        """Initialize CSV file with headers"""
        try:
            with open(self._csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(self._csv_headers)
            logger.info(f"CSV file initialized: {self._csv_filename}")
        except Exception as e:
            logger.error(f"Error initializing CSV file: {e}")

    def _write_candle_to_csv(self, candle: Candle) -> None:
        """Write candle data to CSV file"""
        try:
            with open(self._csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    candle.formatted_time,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume
                ])
            logger.debug(f"Candle written to CSV: {candle}")
        except Exception as e:
            logger.error(f"Error writing to CSV: {e}")
            self._stats.errors_count += 1

    def _is_candle_closed(self, timestamp: int) -> bool:
        """Check if this is a new completed 3-minute candle"""
        if self._last_saved_timestamp is None:
            return True
        
        # A candle is closed when we get a new timestamp (new 3-minute period starts)
        # This means the previous candle period has ended
        return timestamp > self._last_saved_timestamp

    def _decompress_data(self, data: bytes) -> str:
        """Decompress gzip data with error handling"""
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(data), mode='rb') as gz_file:
                return gz_file.read().decode('utf-8')
        except Exception as e:
            logger.error(f"Decompression error: {e}")
            self._stats.errors_count += 1
            return ""

    def _create_candle_from_data(self, kline_data: Any) -> Optional[Candle]:
        """Create Candle object from raw kline data"""
        try:
            # BingX format: {"c": close, "o": open, "h": high, "l": low, "v": volume, "T": timestamp}
            if isinstance(kline_data, dict):
                candle = Candle(
                    open=float(kline_data["o"]),
                    high=float(kline_data["h"]),
                    low=float(kline_data["l"]),
                    close=float(kline_data["c"]),
                    volume=float(kline_data["v"]),
                    timestamp=int(kline_data["T"])  # Note: "T" not "t"
                )
                logger.debug(f"Created candle: {candle}")
                return candle
            else:
                logger.warning(f"Unknown kline data format: {type(kline_data)}")
                return None
        except (KeyError, ValueError, TypeError, IndexError) as e:
            logger.error(f"Error creating candle from data: {e}")
            logger.error(f"Data format: {type(kline_data)}, Data: {kline_data}")
            self._stats.errors_count += 1
            return None

    async def _producer(self) -> None:
        """Producer: WebSocket connection that feeds candles to queue"""
        self._stats.connection_attempts += 1
        logger.info("Producer: Starting WebSocket connection")
        
        try:
            async with websockets.connect(
                self._config.url,
                ping_interval=None,  # Disable automatic ping completely
                ping_timeout=None    # Disable ping timeout
            ) as websocket:
                logger.info('Producer: WebSocket connected successfully')
                self._stats.successful_connections += 1
                
                # Subscribe to channel
                subscription = json.dumps(self._channel.to_dict())
                await websocket.send(subscription)
                logger.info(f"Producer: Subscribed to channel: {self._channel.data_type}")
                
                async for message in websocket:
                    if not self._is_running:
                        logger.info("Producer: Stopping due to shutdown signal")
                        break
                    
                    try:
                        # Handle binary messages (compressed)
                        if isinstance(message, bytes):
                            decoded_message = self._decompress_data(message)
                        else:
                            decoded_message = message
                        
                        if not decoded_message:
                            continue
                        
                        # Handle simple ping/pong (string format like original)
                        if decoded_message.strip() == "Ping":
                            await websocket.send("Pong")
                            logger.debug("Producer: Responded to 'Ping' with 'Pong'")
                            continue
                        
                        # Parse kline data
                        try:
                            data = json.loads(decoded_message)
                            if "data" in data and "dataType" in data and "kline" in data["dataType"]:
                                # Data comes as an array with one object
                                if isinstance(data["data"], list) and len(data["data"]) > 0:
                                    kline_obj = data["data"][0]  # Get first (and only) object from array
                                    candle = self._create_candle_from_data(kline_obj)
                                    if candle:
                                        # Put candle in queue (producer)
                                        await self._raw_queue.put(candle)
                                        self._stats.candles_received += 1
                                        logger.debug(f"Producer: Added candle to queue, total received: {self._stats.candles_received}")
                        except json.JSONDecodeError as e:
                            # Skip empty or invalid JSON messages
                            if decoded_message.strip():  # Only log if message is not empty
                                logger.warning(f"Producer: Failed to parse JSON message: {e}")
                        
                    except Exception as e:
                        logger.error(f"Producer: Error processing message: {e}")
                        self._stats.errors_count += 1
                        
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"Producer: WebSocket connection closed: {e}")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"Producer: WebSocket connection closed with error: {e}")
        except Exception as e:
            logger.error(f"Producer: WebSocket error: {e}")
            self._stats.errors_count += 1

    async def _consumer(self) -> None:
        """Consumer: Process candles from queue"""
        logger.info("Consumer: Started processing candles")
        previous_candle = None
        
        while self._is_running:
            try:
                # Get candle from queue (consumer)
                candle = await self._raw_queue.get()
                
                # Check if we have a completed candle to save
                if self._is_candle_closed(candle.timestamp) and previous_candle is not None:
                    # Save the previous candle (which is now closed)
                    self._candles.append(previous_candle)
                    self._stats.candles_processed += 1
                    
                    # Write to CSV file only for closed candles
                    self._write_candle_to_csv(previous_candle)
                    
                    # Put in processed queue
                    await self._processed_queue.put(previous_candle)
                    
                    logger.info(f"Consumer: Closed candle saved to CSV - {previous_candle}")
                
                # Update tracking
                if self._is_candle_closed(candle.timestamp):
                    self._last_saved_timestamp = candle.timestamp
                
                # Store current candle as the potential next closed candle
                previous_candle = candle
                logger.debug(f"Consumer: Live update (current candle) - {candle}")
                
                # Mark task as done
                self._raw_queue.task_done()
                
            except Exception as e:
                logger.error(f"Consumer: Error processing candle: {e}")
                self._stats.errors_count += 1

    async def start(self) -> None:
        """Start the producer-consumer system"""
        if self._is_running:
            logger.warning("System is already running")
            return
        
        self._is_running = True
        logger.info("Starting producer-consumer system...")
        
        # Initialize CSV file
        self._init_csv_file()
        
        # Start producer and consumer tasks
        self._producer_task = asyncio.create_task(self._producer())
        self._consumer_task = asyncio.create_task(self._consumer())
        logger.info("Producer and consumer tasks started")

    async def stop(self) -> None:
        """Stop the producer-consumer system"""
        if not self._is_running:
            return
        
        logger.info("Stopping producer-consumer system...")
        self._is_running = False
        
        # Cancel tasks
        if self._producer_task:
            self._producer_task.cancel()
            logger.debug("Producer task cancelled")
        if self._consumer_task:
            self._consumer_task.cancel()
            logger.debug("Consumer task cancelled")
        
        # Wait for tasks to complete
        try:
            if self._producer_task:
                await self._producer_task
        except asyncio.CancelledError:
            logger.debug("Producer task completed")
        
        try:
            if self._consumer_task:
                await self._consumer_task
        except asyncio.CancelledError:
            logger.debug("Consumer task completed")
        
        logger.info("Producer-consumer system stopped successfully")

    @property
    def candles(self) -> List[Candle]:
        """Get processed candles"""
        return self._candles.copy()

    @property
    def stats(self) -> CollectorStats:
        """Get collector statistics"""
        return self._stats

async def main():
    """Main function to run the producer-consumer system"""
    logger.info("Starting BingX Candle Collector application")
    collector = BingXProducerConsumer(symbol="BTC-USDT", interval="3m")
    
    try:
        await collector.start()
        
        # Let it run
        while True:
            await asyncio.sleep(3600)  # Sleep for 1 hour
    
    except KeyboardInterrupt:
        logger.info("Received shutdown signal (Ctrl+C)")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        await collector.stop()
        logger.info("Application shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())