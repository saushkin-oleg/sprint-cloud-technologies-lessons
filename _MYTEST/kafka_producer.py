from confluent_kafka import Producer
import json
import socket
import time
import uuid

def error_callback(err):
    print('Something went wrong: {}'.format(err))

def delivery_report(err, msg):
    """Callback для подтверждения доставки сообщения"""
    if err is not None:
        print(f'❌ Ошибка доставки сообщения: {err}')
    else:
        print(f'✅ Сообщение доставлено в топик {msg.topic()} [партиция {msg.partition()}, офсет {msg.offset()}]')

# Ваши параметры
params = {
    'bootstrap.servers': 'rc1a-kj2v5dv6er9ihgl2.mdb.yandexcloud.net:9091',
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': 'C:/Users/o.saushkin/Documents/Programming/YandexInternalRootCA.crt',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'producer_consumer',
    'sasl.password': 'ocm_practicum',
    'error_cb': error_callback,
    'client.id': socket.gethostname()
}

print("=" * 60)
print("ПРОДЮСЕР KAFKA - order-service_orders")
print("=" * 60)
print(f"Брокер: {params['bootstrap.servers']}")
print(f"Пользователь: {params['sasl.username']}")
print(f"Топик: order-service_orders")
print()

try:
    # Создание продюсера
    producer = Producer(params)
    
    # Топик
    topic_name = 'order-service_orders'
    
    # Создаем тестовый заказ
    order = {
        'order_id': str(uuid.uuid4()),
        'user_id': 'user_123',
        'restaurant_id': 'rest_456',
        'items': [
            {'product_id': 'prod_001', 'name': 'Пицца Маргарита', 'quantity': 2, 'price': 450},
            {'product_id': 'prod_002', 'name': 'Кола', 'quantity': 2, 'price': 100}
        ],
        'total_amount': 1100,
        'status': 'created',
        'created_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'delivery_address': 'ул. Пушкина, д. 10'
    }
    
    order_json = json.dumps(order, ensure_ascii=False)
    print(f"Отправляем заказ:")
    print(f"  Ключ: {order['order_id']}")
    print(f"  Значение: {json.dumps(order, indent=2, ensure_ascii=False)}")
    print()
    
    # Отправляем сообщение (используем order_id как ключ для партиционирования)
    producer.produce(
        topic=topic_name,
        key=order['order_id'],
        value=order_json,
        callback=delivery_report
    )
    
    # Триггерим callback
    producer.poll(0)
    
    # Ожидаем доставки
    print("Ожидание доставки сообщения...")
    remaining = producer.flush(30)
    
    if remaining == 0:
        print(f"\n✅ Заказ успешно отправлен в топик {topic_name}")
    else:
        print(f"\n⚠️ {remaining} сообщений не доставлены")
    
except Exception as e:
    print(f"\n❌ Ошибка: {e}")
    
print("\n" + "=" * 60)