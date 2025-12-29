import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import uuid

class CustomJsonFormatter(logging.Formatter):
    """Format logs as JSON for structured logging"""
    
    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add request_id if available in the record
        if hasattr(record, 'request_id'):
            log_data['request_id'] = record.request_id
        
        # Add any extra fields
        if hasattr(record, 'extra'):
            log_data.update(record.extra)
        
        return json.dumps(log_data)

def setup_logging():
    """Configure structured JSON logging to both file and console"""
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    # JSON formatter
    json_formatter = CustomJsonFormatter()
    
    # File handler (rotating, max 10MB per file, keep 5 files)
    file_handler = RotatingFileHandler(
        'logs/app.log',
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(json_formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler (for CloudWatch pickup)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(json_formatter)
    root_logger.addHandler(console_handler)
    
    return root_logger

def get_logger(name):
    """Get a logger with request context"""
    return logging.getLogger(name)
