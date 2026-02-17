#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для подключения к Valkey и изучения данных
"""

import redis
import ssl
import json

def main():
    # Параметры подключения к Valkey
    host = 'c-c9qp1gj6ckquku8n2h93.rw.mdb.yandexcloud.net'  # FQDN хоста
    port = 6380                                              # порт подключения
    password = 'ocm_practicum'                               # пароль для подключения
    ca_path = r'C:\Users\o.saushkin\Documents\Programming\YandexInternalRootCA.crt'  # путь к сертификату
    
    print("=" * 60)
    print("Подключение к Valkey")
    print("=" * 60)
    print(f"Хост: {host}")
    print(f"Порт: {port}")
    print(f"Сертификат: {ca_path}")
    print()
    
    try:
        # Инициализация клиента для подключения к Valkey
        client = redis.StrictRedis(
            host=host,
            port=port,
            password=password,
            ssl=True,
            ssl_ca_certs=ca_path,
            decode_responses=True  # автоматическое декодирование в строки
        )
        
        # Проверка подключения
        print("Проверка подключения...")
        client.ping()
        print("✅ Подключение к Valkey успешно установлено")
        print()
        
        # Ключ для поиска
        key = "ef8c42c19b7518a9aebec106"
        print(f"Поиск ключа: {key}")
        print()
        
        # Получение значения по ключу
        result = client.get(key)
        
        if result is None:
            print(f"❌ Запись с ключом '{key}' не найдена")
            print("\nПроверьте список доступных ключей:")
            
            # Получаем несколько первых ключей для проверки
            keys = client.keys('*')[:5]
            if keys:
                print("Первые 5 ключей в базе:")
                for k in keys:
                    print(f"  - {k}")
            else:
                print("В базе нет ключей")
        else:
            print(f"✅ Запись с ключом '{key}' найдена")
            print()
            print("Значение (сырое):")
            print(result)
            print()
            
            # Пытаемся распарсить как JSON
            try:
                json_result = json.loads(result)
                print("Значение (JSON):")
                print(json.dumps(json_result, indent=2, ensure_ascii=False))
                
                # Если есть поле name, выводим его отдельно
                if isinstance(json_result, dict) and 'name' in json_result:
                    print(f"\nЗначение поля 'name': {json_result['name']}")
            except json.JSONDecodeError:
                print("Значение не является JSON")
        
    except redis.AuthenticationError:
        print("❌ Ошибка аутентификации - неверный пароль")
    except redis.ConnectionError as e:
        print(f"❌ Ошибка подключения: {e}")
    except FileNotFoundError:
        print(f"❌ Файл сертификата не найден: {ca_path}")
    except Exception as e:
        print(f"❌ Неизвестная ошибка: {e}")

if __name__ == '__main__':
    main()