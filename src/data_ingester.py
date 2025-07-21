import asyncio
import json
from datetime import datetime
from elasticsearch import AsyncElasticsearch, ConnectionError, NotFoundError, ApiError

# Змінено: Змінна es тепер ініціалізується як None. 
# Клієнт буде створений асинхронно функцією initialize_es_client()
es: AsyncElasticsearch = None 

INDEX_NAME = "scan_results"

# Функція для асинхронної ініціалізації клієнта Elasticsearch
async def initialize_es_client():
    global es
    if es: # Якщо клієнт вже існує, закриваємо його, щоб уникнути Unclosed client session
        try:
            await es.close()
            print("Попередній клієнт Elasticsearch закрито.")
        except Exception as e:
            print(f"Помилка при закритті попереднього клієнта ES: {e}")
            
    try:
        es = AsyncElasticsearch([{'host': 'localhost', 'port': 9284, 'scheme': 'http'}])
        if await es.ping():
            print("Успішно підключено до Elasticsearch.")
            return True
        else:
            print("Попередження: Не вдалося підключитися до Elasticsearch. Перевірте, чи він запущений на http://localhost:9284")
            es = None # Якщо ping не вдається, вважаємо, що es не підключений
            return False
    except ConnectionError as e:
        print(f"Помилка підключення до Elasticsearch: {e}")
        es = None
        return False
    except Exception as e:
        print(f"Критична помилка при ініціалізації клієнта Elasticsearch: {e}")
        es = None
        return False

# Функція для закриття клієнта Elasticsearch
async def close_es_client():
    global es
    if es:
        print("Закриття клієнта Elasticsearch...")
        await es.close()
        print("Клієнт Elasticsearch закрито.")
        es = None

# ---- Заглушки для збагачення даних ----
def get_geolocation(ip_address):
    """
    Заглушка для функції отримання геолокації за IP-адресою.
    У реальному проекті тут буде інтеграція з GeoLite2 або іншим сервісом.
    """
    # Приклад даних для локальних IP або відомих заглушок
    if ip_address.startswith("127."):
        return {
            "country_code": "LO", # Localhost
            "country_name": "Localhost",
            "city": "Loopback",
            "latitude": 0.0,
            "longitude": 0.0,
            "asn": "AS0",
            "organization": "Loopback Network"
        }
    # Можете додати більше логіки для інших IP-адрес
    return {
        "country_code": "XX",
        "country_name": "Unknown",
        "city": "Unknown",
        "latitude": None,
        "longitude": None,
        "asn": None,
        "organization": None
    }

def infer_service_info(banner, port):
    """
    Заглушка для функції визначення назви та версії сервісу з банера та порту.
    У реальному проекті тут будуть регулярні вирази та/або бази даних сигнатур.
    """
    banner_lower = banner.lower()
    if "ssh" in banner_lower:
        return "SSH", "OpenSSH"
    elif "http" in banner_lower or port == 80 or port == 443:
        if "nginx" in banner_lower:
            return "HTTP", "Nginx"
        elif "apache" in banner_lower:
            return "HTTP", "Apache HTTPD"
        return "HTTP", "Unknown HTTP"
    elif "ftp" in banner_lower or port == 21:
        return "FTP", "Unknown FTP"
    elif "microsoft" in banner_lower and port == 445:
        return "SMB", "Microsoft SMB"
    elif port == 3389:
        return "RDP", "Microsoft RDP"
    elif port == 23:
        return "Telnet", "Telnet Server"
    
    return "Unknown", None

# ---- Основна логіка введення даних ----
def format_document(scan_result):
    """
    Форматує один результат сканування у документ Elasticsearch.
    scan_result: {'ip': '127.0.0.1', 'port': 80, 'banner': 'HTTP/1.1 200 OK'}
    """
    ip_address = scan_result['ip']
    port = scan_result['port']
    banner = scan_result['banner']
    
    now = datetime.utcnow().isoformat(timespec='milliseconds') + "Z" # ISO 8601 формат для Elasticsearch

    geolocation_data = get_geolocation(ip_address)
    service_name, service_version = infer_service_info(banner, port)

    document = {
        "ip_address": ip_address,
        "port": port,
        "protocol": "tcp", # Припускаємо TCP для цього прототипу
        "banner": banner,
        "timestamp_first_seen": now,
        "timestamp_last_seen": now,
        "geolocation": geolocation_data,
        "service_name_inferred": service_name,
        "version_inferred": service_version,
        "tags": [] # Поки порожньо, можна додати пізніше
    }
    return document

async def ingest_scan_results(results_queue):
    """
    Асинхронно отримує результати сканування з черги, збагачує їх та індексує в Elasticsearch.
    """
    global es # Отримуємо доступ до глобальної змінної es
    if es is None:
        print("Модуль введення даних не може працювати: Elasticsearch не підключено.")
        # Чекати на сигнал завершення, щоб коректно завершити чергу
        while True:
            item = await results_queue.get()
            if item is None:
                results_queue.task_done()
                break
            results_queue.task_done() # Позначаємо завдання виконаним навіть без індексації
        return

    batch = []
    batch_size = 50 # Розмір пачки для індексації (можна налаштувати)

    print("Модуль введення даних запущено і очікує результатів...")

    while True:
        scan_result = await results_queue.get()
        if scan_result is None: # Сигнал завершення
            if batch: # Індексуємо останню неповну пачку
                await index_batch(batch)
            results_queue.task_done()
            break

        document = format_document(scan_result)
        batch.append(document)

        if len(batch) >= batch_size:
            await index_batch(batch)
            batch = [] # Очистити пачку після індексації
        
        results_queue.task_done()

async def index_batch(documents):
    """
    Виконує масову індексацію документів в Elasticsearch.
    """
    if es is None or not documents: # Змінено: перевіряємо es, оскільки він може бути None
        print("Elasticsearch клієнт недоступний для індексації.")
        return

    actions = [
        {
            "_index": INDEX_NAME,
            "_id": f"{doc['ip_address']}-{doc['port']}", # Унікальний ID для оновлення документів
            "_source": doc
        }
        for doc in documents
    ]

    try:
        # Використовуємо bulk API для ефективної індексації
        # 'op_type': 'index' - створити або замінити документ, якщо він вже існує.
        # Це корисно для оновлення timestamp_last_seen при повторному скануванні.
        success, failed = await es.options(request_timeout=30).bulk(operations=actions, op_type='index')
        
        if failed:
            print(f"Помилка індексації деяких документів: {len(failed)} невдалих.")
            # print(failed) # Для детального дебагу помилок індексації
        else:
            print(f"Успішно проіндексовано {len(documents)} документів в Elasticsearch.")
    except ApiError as e:
        print(f"Помилка API Elasticsearch під час індексації: {e.info}")
    except ConnectionError as e:
        print(f"Помилка підключення до Elasticsearch під час індексації: {e}. Перевірте з'єднання.")
    except Exception as e:
        print(f"Непередбачена помилка під час індексації: {e}")

# Функція для створення індексу в Elasticsearch (викликається один раз при запуску програми)
async def create_index_if_not_exists():
    global es # Отримуємо доступ до глобальної змінної es
    if es is None:
        print("Не можу створити індекс: Elasticsearch не підключено.")
        return False
    
    if not await es.indices.exists(index=INDEX_NAME):
        print(f"Індекс '{INDEX_NAME}' не знайдено, створюємо...")
        # Визначаємо маппінг для індексу
        mapping = {
            "mappings": {
                "properties": {
                    "ip_address": {"type": "ip"},
                    "port": {"type": "integer"},
                    "protocol": {"type": "keyword"},
                    "banner": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                    "timestamp_first_seen": {"type": "date"},
                    "timestamp_last_seen": {"type": "date"},
                    "geolocation": {
                        "properties": {
                            "country_code": {"type": "keyword"},
                            "country_name": {"type": "keyword"},
                            "city": {"type": "keyword"},
                            "latitude": {"type": "float"},
                            "longitude": {"type": "float"},
                            "asn": {"type": "keyword"},
                            "organization": {"type": "keyword"}
                        }
                    },
                    "service_name_inferred": {"type": "keyword"},
                    "version_inferred": {"type": "keyword"},
                    "tags": {"type": "keyword"},
                    "vulnerabilities": {"type": "nested"} # Використовувати "nested" для складних об'єктів
                }
            }
        }
        try:
            await es.indices.create(index=INDEX_NAME, body=mapping)
            print(f"Індекс '{INDEX_NAME}' успішно створено.")
            return True
        except ApiError as e:
            print(f"Помилка API при створенні індексу: {e.info}")
            return False
        except Exception as e:
            print(f"Непередбачена помилка при створенні індексу: {e}")
            return False
    else:
        print(f"Індекс '{INDEX_NAME}' вже існує.")
        return True

if __name__ == "__main__":
    # Приклад автономного тестування модуля введення даних
    async def test_ingester():
        # Створюємо тимчасову чергу та поміщаємо в неї тестові дані
        test_queue = asyncio.Queue()
        await test_queue.put({'ip': '127.0.0.1', 'port': 80, 'banner': 'HTTP/1.1 200 OK'})
        await test_queue.put({'ip': '127.0.0.1', 'port': 22, 'banner': 'SSH-2.0-OpenSSH_8.2p1 Ubuntu-4ubuntu0.1'})
        await test_queue.put({'ip': '192.168.1.1', 'port': 443, 'banner': 'Nginx/1.18.0 (Ubuntu)'})
        await test_queue.put({'ip': '192.168.1.2', 'port': 21, 'banner': '220 ProFTPD 1.3.6 Server (Debian) [::ffff:192.168.1.2]'})

        # Ініціалізуємо ES клієнт перед використанням
        if not await initialize_es_client():
            print("Тестування ingester не може продовжитись без підключення до Elasticsearch.")
            return

        # Створюємо індекс, якщо його немає
        await create_index_if_not_exists()

        # Запускаємо ingester
        ingester_task = asyncio.create_task(ingest_scan_results(test_queue))
        
        # Сигналізуємо ingester'у про завершення після того, як всі дані поміщені
        await test_queue.put(None)
        await test_queue.join() # Чекаємо, поки всі завдання з черги будуть виконані
        await ingester_task # Чекаємо завершення самого ingester'а
        print("\nТестове введення даних завершено.")
        await close_es_client() # Закриваємо клієнт після тестування

    asyncio.run(test_ingester())
