import uuid

from flask import request, jsonify

from models import get_order, save_order

from constant import metrics

from custom_logging import logger
from processor import order_queue


@app.route('/api/orders', methods=['POST'])
def create_order():
    """API endpoint to create a new order"""
    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ['user_id', 'item_ids', 'total_amount']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400

        # Generate order_id if not provided
        if 'order_id' not in data:
            data['order_id'] = str(uuid.uuid4())

        # Set initial status to PENDING
        data['status'] = 'PENDING'
        metrics['status_counts']['PENDING'] += 1

        # Save to database
        save_order(data)

        # Add to processing queue
        order_queue.put(data)

        return jsonify({
            'order_id': data['order_id'],
            'status': 'PENDING',
            'message': 'Order received and queued for processing'
        }), 201

    except Exception as e:
        logger.error(f"Error creating order: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/orders/<order_id>', methods=['GET'])
def get_order_status(order_id):
    """API endpoint to get status of an order"""
    try:
        order_data = get_order(order_id)

        if order_data:
            return jsonify({
                'order_id': order_data['order_id'],
                'user_id': order_data['user_id'],
                'item_ids': order_data['item_ids'],
                'total_amount': order_data['total_amount'],
                'status': order_data['status'],
                'created_at': order_data['created_at'],
                'updated_at': order_data['updated_at']
            }), 200
        else:
            return jsonify({'error': 'Order not found'}), 404

    except Exception as e:
        logger.error(f"Error getting order status: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    """API endpoint to get system metrics"""
    try:
        # Calculate average processing time
        avg_processing_time = 0
        if metrics['processing_times']:
            avg_processing_time = sum(metrics['processing_times']) / len(metrics['processing_times'])

        return jsonify({
            'total_orders_processed': metrics['total_orders_processed'],
            'average_processing_time': round(avg_processing_time, 2),
            'status_counts': metrics['status_counts']
        }), 200

    except Exception as e:
        logger.error(f"Error getting metrics: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500
