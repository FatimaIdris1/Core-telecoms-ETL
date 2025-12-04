import logging
import sys
import json
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional, Dict

# ===================================
# LOGGING SETUP
# ===================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log")
    ]
)
logger = logging.getLogger(__name__)


# ===================================
# OPERATION METADATA CLASS
# ===================================
@dataclass
class OperationMetadata:
    operation_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    status: str = "RUNNING"
    records_processed: int = 0
    records_success: int = 0
    records_failed: int = 0
    records_skipped = 0   # <-- add this

    error_message: Optional[str] = None
    additional_info: Optional[Dict] = None

    def complete(self, status="SUCCESS", error=None):
        self.end_time = datetime.now()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        self.status = status
        if error:
            self.error_message = error

    def to_dict(self):
        return {
            **asdict(self),
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None
        }

    def log_summary(self):
        logger.info("=" * 60)
        logger.info(f"OPERATION: {self.operation_name}")
        logger.info(f"Status: {self.status}")
        logger.info(f"Duration: {self.duration_seconds:.2f} seconds")
        logger.info(f"Records Processed: {self.records_processed}")
        logger.info(f"Records Success: {self.records_success}")
        logger.info(f"Records Failed: {self.records_failed}")
        logger.info(f"Records Skipped: {self.records_skipped}")
        if self.error_message:
            logger.info(f"Error: {self.error_message}")
        if self.additional_info:
            logger.info(json.dumps(self.additional_info, indent=2))
        logger.info("=" * 60)
