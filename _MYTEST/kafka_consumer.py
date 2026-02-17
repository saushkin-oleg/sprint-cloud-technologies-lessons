from confluent_kafka import Consumer
import json

def error_callback(err):
    print('Ошибка: {}'.format(err))

params = {
    'bootstrap.servers': 'rc1a-kj2v5dv6er9ihgl2.mdb.yandexcloud.net:9091',
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': 'C:/Users/o.saushkin/Documents/Programming/YandexInternalRootCA.crt',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'producer_consumer',
    'sasl.password': 'ocm_practicum',
    'error_cb': error_callback,
    'group.id': 'order-service-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

print("=" * 60)
print("КОНСЮМЕР KAFKA - order-service_orders")
print("=" * 60)
print(f"Брокер: {params['bootstrap.servers']}")
print(f"Пользователь: {params['sasl.username']}")
print(f"Топик: order-service_orders")
print("=" * 60)
print()

consumer = Consumer(params)
topic = 'order-service_orders'
consumer.subscribe([topic])

print(f"Чтение заказов из топика {topic}...")
print("Нажмите Ctrl+C для выхода\n")

order_count = 0

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Ошибка: {msg.error()}")
            continue
        
        key = msg.key().decode('utf-8') if msg.key() else None
        value = msg.value().decode('utf-8')
        
        order_count += 1
        print(f"\n{'='*50}")
        print(f"ЗАКАЗ #{order_count}")
        print(f"{'='*50}")
        print(f"  Топик: {msg.topic()}")
        print(f"  Партиция: {msg.partition()}")
        print(f"  Офсет: {msg.offset()}")
        print(f"  ID заказа: {key}")
        
        try:
            # Парсим JSON заказа
            order = json.loads(value)
            print(f"\n  Детали заказа:")
            print(f"    Пользователь: {order.get('user_id', 'N/A')}")
            print(f"    Ресторан: {order.get('restaurant_id', 'N/A')}")
            print(f"    Сумма: {order.get('total_amount', 0)} руб.")
            print(f"    Статус: {order.get('status', 'N/A')}")
            print(f"    Время создания: {order.get('created_at', 'N/A')}")
            print(f"    Адрес: {order.get('delivery_address', 'N/A')}")
            
            print(f"\n  Товары:")
            for i, item in enumerate(order.get('items', []), 1):
                print(f"    {i}. {item.get('name')} x{item.get('quantity')} = {item.get('price') * item.get('quantity', 1)} руб.")
                
        except json.JSONDecodeError:
            print(f"\n  Значение (не JSON): {value}")
        
except KeyboardInterrupt:
    print(f"\n\nЗавершение работы консюмера. Всего получено заказов: {order_count}")
finally:
    consumer.close()
    print("Консюмер закрыт")