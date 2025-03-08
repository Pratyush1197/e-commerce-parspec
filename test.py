import random
import uuid

from flask import request, jsonify, app

from constant import metrics
from custom_logging import logger

from models import save_order
from processor import order_queue


@app.route('/api/load-test', methods=['POST'])
def load_test():
    """API endpoint to simulate load with multiple concurrent orders"""
    try:
        data = request.get_json()
        num_orders = data.get('num_orders', 100)

        if num_orders > 1000:
            return jsonify({'error': 'Maximum 1000 orders allowed for load test'}), 400

        generated_order_ids = []

        # Generate and queue orders
        for i in range(num_orders):
            order_id = f"test-order-{uuid.uuid4()}"
            order_data = {
                'order_id': order_id,
                'user_id': f"test-user-{i % 100}",
                'item_ids': [f"item-{j}" for j in range(1, random.randint(2, 5))],
                'total_amount': round(random.uniform(10, 1000), 2),
                'status': 'PENDING'
            }

            # Save to database
            save_order(order_data)

            # Add to processing queue
            order_queue.put(order_data)

            # Update metrics
            metrics['status_counts']['PENDING'] += 1

            generated_order_ids.append(order_id)

        return jsonify({
            'message': f'Created {num_orders} test orders',
            'order_ids': generated_order_ids
        }), 201

    except Exception as e:
        logger.error(f"Error in load test: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500
