import json
import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any

from constant import metrics
from custom_logging import logger


def load_metrics_from_db() -> None:
    """Load metrics from database on application startup"""
    conn = sqlite3.connect('orders.db')
    cursor = conn.cursor()

    cursor.execute('SELECT metric_name, metric_value FROM metrics')
    rows = cursor.fetchall()

    for row in rows:
        metric_name, metric_value = row

        if metric_name == 'total_orders_processed':
            metrics['total_orders_processed'] = int(metric_value)
        elif metric_name == 'avg_processing_time':
            # We don't directly restore processing_times array, just using this for logging
            logger.info(f"Loaded average processing time: {metric_value}")
        elif metric_name.startswith('status_count_'):
            status = metric_name.replace('status_count_', '')
            metrics['status_counts'][status] = int(metric_value)

    conn.close()
    logger.info("Metrics loaded from database")


def update_metrics__in_db() -> None:
    """Update metrics in database for persistence"""
    conn = sqlite3.connect('orders.db')
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    # Update total orders processed
    cursor.execute(
        '''
        INSERT OR REPLACE INTO metrics (metric_name, metric_value, updated_at)
        VALUES (?, ?, ?)
        ''',
        ('total_orders_processed', str(metrics['total_orders_processed']), now)
    )

    avg_time = 0
    if metrics['processing_times']:
        avg_time = sum(metrics['processing_times']) / len(metrics['processing_times'])

    cursor.execute(
        '''
        INSERT OR REPLACE INTO metrics (metric_name, metric_value, updated_at)
        VALUES (?, ?, ?)
        ''',
        ('avg_processing_time', str(avg_time), now)
    )

    # Update status counts
    for status, count in metrics['status_counts'].items():
        cursor.execute(
            '''
            INSERT OR REPLACE INTO metrics (metric_name, metric_value, updated_at)
            VALUES (?, ?, ?)
            ''',
            (f'status_count_{status}', str(count), now)
        )

    conn.commit()
    conn.close()


def load_metrics_from_db() -> None:
    """Load metrics from database on application startup"""
    conn = sqlite3.connect('orders.db')
    cursor = conn.cursor()

    cursor.execute('SELECT metric_name, metric_value FROM metrics')
    rows = cursor.fetchall()

    for row in rows:
        metric_name, metric_value = row

        if metric_name == 'total_orders_processed':
            metrics['total_orders_processed'] = int(metric_value)
        elif metric_name == 'avg_processing_time':
            # We don't directly restore processing_times array, just using this for logging
            logger.info(f"Loaded average processing time: {metric_value}")
        elif metric_name.startswith('status_count_'):
            status = metric_name.replace('status_count_', '')
            metrics['status_counts'][status] = int(metric_value)

    conn.close()
    logger.info("Metrics loaded from database")


def get_order(order_id: str) -> Optional[Dict[str, Any]]:
    """Get order details from database"""
    conn = sqlite3.connect('orders.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM orders WHERE order_id = ?', (order_id,))
    row = cursor.fetchone()

    conn.close()

    if row:
        order_data = dict(row)
        # Convert JSON string back to list
        order_data['item_ids'] = json.loads(order_data['item_ids'])
        return order_data

    return None


def update_order_status(order_id: str, status: str) -> bool:
    """Update order status in database"""
    conn = sqlite3.connect('orders.db')
    cursor = conn.cursor()

    now = datetime.now().isoformat()

    cursor.execute(
        '''
        UPDATE orders 
        SET status = ?, updated_at = ?
        WHERE order_id = ?
        ''',
        (status, now, order_id)
    )

    success = cursor.rowcount > 0
    conn.commit()
    conn.close()

    if success:
        logger.info(f"Order {order_id} status updated to {status}")
    else:
        logger.warning(f"Failed to update status for order {order_id}")

    return success


def save_order(order_data: Dict[str, Any]) -> None:
    """Save order to database"""
    conn = sqlite3.connect('orders.db')
    cursor = conn.cursor()

    # Convert item_ids list to JSON string for storage
    item_ids_json = json.dumps(order_data['item_ids'])

    now = datetime.now().isoformat()

    cursor.execute(
        '''
        INSERT INTO orders (order_id, user_id, item_ids, total_amount, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            order_data['order_id'],
            order_data['user_id'],
            item_ids_json,
            order_data['total_amount'],
            order_data['status'],
            now,
            now
        )
    )

    conn.commit()
    conn.close()
    logger.info(f"Order {order_data['order_id']} saved to database")





