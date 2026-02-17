import json
from typing import Dict
import redis


class RedisClient:
    def __init__(self, host: str, port: int, password: str, cert_path: str) -> None:
        # Инициализация клиента Redis с SSL подключением
        self._client = redis.StrictRedis(
            host=host,
            port=port,
            password=password,
            ssl=True,
            ssl_ca_certs=cert_path,
            decode_responses=True  # Автоматическое декодирование ответов в строки
        )
        
    def set(self, k: str, v: Dict):
        # Преобразование словаря в JSON-строку и запись в Redis
        json_value = json.dumps(v, ensure_ascii=False)
        self._client.set(k, json_value)

    def get(self, k: str) -> Dict:
        # Получение JSON-строки из Redis и преобразование обратно в словарь
        json_value = self._client.get(k)
        if json_value:
            return json.loads(json_value)
        return None