import json
from typing import Dict, Optional
from confluent_kafka import Consumer


class KafkaConsumer:
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 group: str,
                 cert_path: str
                 ) -> None:
        # Параметры подключения к Kafka
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'group.id': group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'client.id': f'consumer-{group}'
        }

        self.consumer = Consumer(params)
        self.consumer.subscribe([topic])

    def consume(self, timeout: float = 3.0) -> Optional[Dict]:
        # Получение сообщения из Kafka
        msg = self.consumer.poll(timeout=timeout)
        
        # Если сообщений нет
        if msg is None:
            return None
            
        # Если есть ошибка
        if msg.error():
            print(f"Ошибка Kafka: {msg.error()}")
            return None
            
        # Декодирование сообщения - ИСПРАВЛЕНО: decode() без аргументов
        try:
            value = msg.value().decode()  # Убрали 'utf-8' так как decode() не принимает аргументов
            return json.loads(value)
        except json.JSONDecodeError:
            # Если не JSON, возвращаем как словарь с raw значением
            return {"raw_value": value}
        except Exception as e:
            print(f"Ошибка при обработке сообщения: {e}")
            return None
    
    def close(self):
        """Закрытие соединения с Kafka"""
        if self.consumer:
            self.consumer.close()


# Пример использования
if __name__ == "__main__":
    # Ваши параметры подключения
    HOST = "rc1a-kj2v5dv6er9ihgl2.mdb.yandexcloud.net"
    PORT = 9091
    USER = "producer_consumer"
    PASSWORD = "ocm_practicum"
    TOPIC = "order-service_orders"
    GROUP = "python-consumer-group"
    CERT_PATH = "C:/Users/o.saushkin/Documents/Programming/YandexInternalRootCA.crt"  # Для Windows
    
    # Создание потребителя
    consumer = KafkaConsumer(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        topic=TOPIC,
        group=GROUP,
        cert_path=CERT_PATH
    )
    
    print(f"✅ Подключено к Kafka. Топик: {TOPIC}, Группа: {GROUP}")
    print("Ожидание сообщений...")
    
    try:
        # Получаем одно сообщение для теста
        message = consumer.consume(timeout=5.0)
        if message:
            print("\n📨 Получено сообщение:")
            print(json.dumps(message, indent=2, ensure_ascii=False))
        else:
            print("❌ Нет сообщений за указанный timeout")
            
    except KeyboardInterrupt:
        print("\n⏹️ Получен сигнал прерывания")
    finally:
        consumer.close()
        print("🔒 Соединение закрыто")