import asyncio
# atexit більше не потрібен, оскільки клієнт керується async with
from flask import Flask, request, jsonify
from elasticsearch import AsyncElasticsearch, ConnectionError, NotFoundError, ApiError

app = Flask(__name__)

# Змінено: es_client_api більше не є глобальним і не ініціалізується тут.
# Він буде створюватися всередині кожної функції представлення.
INDEX_NAME = "scan_results"

# Функції _init_es_client_api_sync та _close_es_client_api_async більше не потрібні,
# оскільки керування клієнтом відбувається через async with у функціях представлення.

@app.route('/')
async def home(): # Робимо функцію асинхронною
    return "Ласкаво просимо до ScanEngine! Використовуйте /search для пошуку."

@app.route('/search', methods=['GET'])
async def search_data():
    # Змінено: Створення клієнта Elasticsearch всередині функції представлення
    # Це гарантує, що клієнт буде коректно створений і закритий для кожного запиту.
    async with AsyncElasticsearch([{'host': '172.18.144.1', 'port': 9200, 'scheme': 'http'}]) as es_client_per_request:
        try:
            if not await es_client_per_request.ping():
                return jsonify({"error": "Elasticsearch недоступний. Перевірте його роботу."}), 500
        except ConnectionError as e:
            print(f"Помилка підключення Elasticsearch для запиту: {e}")
            return jsonify({"error": "Не вдалося підключитися до Elasticsearch."}), 500
        
        # Спрощено запит до match_all для діагностики
        es_query = {
            "query": {"match_all": {}},
            "from": 0, # Починаємо з 0
            "size": 10, # Обмежуємо 10 результатами для початку
            "sort": [{"timestamp_last_seen": {"order": "desc"}}]
        }

        try:
            response = await es_client_per_request.search(index=INDEX_NAME, body=es_query)
            
            hits = response['hits']['hits']
            total_hits = response['hits']['total']['value']

            results = [hit['_source'] for hit in hits]
            
            return jsonify({
                "results": results,
                "total": total_hits,
                "page": 1, 
                "size": 10
            })
        except (ConnectionError, ApiError) as e:
            print(f"Помилка Elasticsearch API під час пошуку: {e}")
            # traceback.print_exc() # Залишаємо для дебагу, якщо потрібно
            return jsonify({"error": "Помилка сервера під час пошуку даних."}), 500
        except Exception as e:
            print(f"Непередбачена помилка під час пошуку: {e}")
            # traceback.print_exc() # Залишаємо для дебагу, якщо потрібно
            return jsonify({"error": "Невідома помилка сервера."}), 500

@app.route('/device/<ip_address>/<port>', methods=['GET'])
async def get_device_details(ip_address, port):
    async with AsyncElasticsearch([{'host': '172.18.144.1', 'port': 9200, 'scheme': 'http'}]) as es_client_per_request:
        try:
            if not await es_client_per_request.ping():
                return jsonify({"error": "Elasticsearch недоступний. Перевірте його роботу."}), 500
        except ConnectionError as e:
            print(f"Помилка підключення Elasticsearch для запиту: {e}")
            return jsonify({"error": "Не вдалося підключитися до Elasticsearch."}), 500
        
        device_id = f"{ip_address}-{port}"
        try:
            response = await es_client_per_request.get(index=INDEX_NAME, id=device_id)
            if response['found']:
                return jsonify(response['_source'])
            else:
                return jsonify({"error": "Пристрій не знайдено."}), 404
        except NotFoundError:
            return jsonify({"error": "Пристрій не знайдено."}), 404
        except (ConnectionError, ApiError) as e:
            print(f"Помилка Elasticsearch API під час отримання деталей пристрою: {e}")
            # traceback.print_exc() # Залишаємо для дебагу, якщо потрібно
            return jsonify({"error": "Помилка сервера під час отримання деталей."}), 500
        except Exception as e:
            print(f"Непередбачена помилка під час отримання деталей: {e}")
            # traceback.print_exc() # Залишаємо для дебагу, якщо потрібно
            return jsonify({"error": "Невідома помилка сервера."}), 500

if __name__ == '__main__':
    # Змінено: Інструкції для запуску Hypercorn, без asyncio.run тут
    print("Для запуску веб-інтерфейсу (API) ScanEngine, виконайте в терміналі:")
    print("1. Переконайтеся, що ви активували віртуальне середовище.")
    print("2. Встановіть Hypercorn, якщо ще не встановлено: pip install hypercorn")
    print("3. Запустіть сервер: hypercorn src.api:app --bind 0.0.0.0:5000 --worker-class asyncio --workers 1")
    print("\nПісля запуску, перейдіть до http://127.0.0.1:5000/search?q=<ваш_запит>")
