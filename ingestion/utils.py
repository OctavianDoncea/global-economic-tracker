"""
Shared utilities: logging, retry decorator, raw file storage, state management
"""

import json
import logging
import time
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

# Logging
def setup_logging(name: str) -> logging.Logger:
    """Create a consistently formatted logger"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s | %(name)-12s | %(levelname)-7s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

# Retry decorator
def retry(max_attempts: int = 3, delay: int = 5):
    """Retry a function on exception, with a fixed delay between attempts."""
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)

            for attempt in range(1, max_attempts+1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        logger.error(f'{func.__name__} failed after {max_attempts} attempts: {e}')
                        raise
                    logger.warning(
                        f'{func.__name__} attempt {attempt}/{max_attempts} failed: {e}'
                        f'Retrying in {delay}s...'
                    )
                    time.sleep(delay)
            
        return wrapper
    return decorator

# Raw file storage
def _raw_dir(source:str) -> Path:
    """Get (and create) the raw data directory for a source."""
    path = Path('data/raw') / source
    path.mkdir(parents=True, exist_ok=True)

    return path

def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d_%H%M%S')

def save_raw_json(data, source: str, label: str) -> Path:
    """Save a raw API response as a timestamped JSON file."""
    filepath = _raw_dir(source) / f'{label}_{_timestamp()}.json'
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)
    
    return filepath

def save_raw_xml(content: str, source: str, label: str) -> Path:
    """Save raw XML content as a timestamped file"""
    filepath = _raw_dir(source) / f'{label}_{_timestamp}.xml'
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return filepath

def save_raw_csv(content: str, source: str, label: str) -> Path:
    """Save raw CSV content as a timestamped file"""
    filepath = _raw_dir(source) / f'{label}_{_timestamp()}.csv'
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return filepath

# State management (for incremental loads)
def _state_dir() -> Path:
    path = Path('data/state')
    path.mkdir(parents=True, exist_ok=True)
    return path

def load_state(source: str) -> dict:
    """Load the state file for source. Returns empty dict if no state exists."""
    state_file = _state_dir() / f'{source}_state.json'
    if state_file.exists():
        with open(state_file, 'r') as f:
            return json.load(f)
    
    return {}

def save_state(source: str, state: dict):
    """Persist the state dict for a source."""
    state_file = _state_dir() / f'{source}_state.json'
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)