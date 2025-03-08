# app.py - Main application file
import sqlite3

from flask import Flask
import threading
import queue
from custom_logging import logger

from models import load_metrics_from_db
from processor import process_orders

app = Flask(__name__)


def init_db():
    """Initialize the database with required tables"""
    conn = sqlite3.connect('orders.db')
    cursor = conn.cursor()

    # Create orders table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        item_ids TEXT NOT NULL,
        total_amount REAL NOT NULL,
        status TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )
    ''')

    # Create metrics table for persistent metrics
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metric_name TEXT NOT NULL,
        metric_value TEXT NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )
    ''')

    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")



def initialize_app():
    init_db()
    load_metrics_from_db()

    num_worker_threads = 4
    for i in range(num_worker_threads):
        worker = threading.Thread(target=process_orders, daemon=True)
        worker.start()
        logger.info(f"Started worker thread {i + 1}")


if __name__ == '__main__':
    initialize_app()
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)