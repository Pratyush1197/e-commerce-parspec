# Order processing worker
import queue
import time

from constant import metrics

from models import update_order_status, update_metrics_in_db

order_queue = queue.Queue()


## This module is to maintain the state machine i.e PENDING -> PROCESSING -> COMPLETED

def move_status_to_processing(order):
    update_order_status(order['order_id'], 'PROCESSING')
    metrics['status_counts']['PENDING'] -= 1
    metrics['status_counts']['PROCESSING'] += 1


def move_status_to_completed(order):
    update_order_status(order['order_id'], 'COMPLETED')
    metrics['status_counts']['PROCESSING'] -= 1
    metrics['status_counts']['COMPLETED'] += 1


def process_orders():
    while True:
        try:
            order = order_queue.get(timeout=1)

            move_status_to_processing(order)

            import random

            processing_time = random.uniform(1, 3)

            start_time = time.time()
            time.sleep(processing_time)
            actual_time = time.time() - start_time

            move_status_to_completed(order)

            metrics['total_orders_processed'] += 1
            metrics['processing_times'].append(actual_time)

            if metrics['total_orders_processed'] % 10 == 0:
                update_metrics_in_db()

            order_queue.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            return
