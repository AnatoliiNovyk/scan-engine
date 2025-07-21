import asyncio
from flask import Flask, request, jsonify
from elasticsearch import AsyncElasticsearch, ConnectionError, NotFoundError, ApiError

app = Flask(__name__)

# Змінна es тепер ініціалізується як None.
# Клієнт буде створений і закритий в хуках життєвого циклу програми.
es_client_api: AsyncElasticsearch = None # Перейменували, щоб уникнути конфлікту імен

INDEX_NAME = "scan_results"

@app.before_serving
async def startup_event():
    """
    Викликається перед першим запитом для ініціалізації клієнта Elasticsearch.
    Цей хук добре працює з ASGI серверами, такими як Hypercorn.
    """
    global es_client_api
    try:
        es_client_api = AsyncElasticsearch([{'host': 'localhost', 'port': 9284, 'scheme': 'http'}])
        if await es_client_api.ping():
            print("Веб-інтерфейс: Успішно підключено до Elasticsearch.")
        else:
            print("Веб-інтерфейс: Попередження: Не вдалося підключитися до Elasticsearch. Функціонал пошуку може бути обмежений.")
            es_client_api = None
    except ConnectionError as e:
        print(f"Веб-інтерфейс: Помилка підключення до Elasticsearch під час запуску: {e}. Функціонал пошуку може бути обмежений.")
        es_client_api = None
    except Exception as e:
        print(f"Веб-інтерфейс: Критична помилка при ініціалізації клієнта Elasticsearch під час запуску: {e}. Функціонал пошуку може бути обмежений.")
        es_client_api = None

@app.teardown_serving
async def shutdown_event():
    """
    Викликається після завершення роботи сервера для закриття клієнта Elasticsearch.
    """
    global es_client_api
    if es_client_api:
        print("Веб-інтерфейс: Закриття клієнта Elasticsearch...")
        await es_client_api.close()
        print("Веб-інтерфейс: Клієнт Elasticsearch закрито.")
        es_client_api = None

@app.route('/')
async def home(): # Робимо функцію асинхронною
    return "Ласкаво просимо до ScanEngine! Використовуйте /search для пошуку."

@app.route('/search', methods=['GET'])
async def search_data():
    if es_client_api is None:
        return jsonify({"error": "Elasticsearch недоступний. Будь ласка, запустіть його та перевірте підключення веб-інтерфейсу."}), 500

    query_string = request.args.get('q', '') # Пошуковий запит
    page = int(request.args.get('page', 1))
    size = int(request.args.get('size', 10))

    from_ = (page - 1) * size
    if from_ < 0: from_ = 0

    es_query = {
        "query": {
            "bool": {
                "must": []
            }
        },
        "from": from_,
        "size": size,
        "sort": [{"timestamp_last_seen": {"order": "desc"}}]
    }

    if query_string:
        es_query["query"]["bool"]["must"].append({
            "multi_match": {
                "query": query_string,
                "fields": [
                    "ip_address", 
                    "port", 
                    "banner", 
                    "service_name_inferred", 
                    "geolocation.country_name", 
                    "geolocation.city",
                    "tags"
                ],
                "fuzziness": "AUTO"
            }
        })
    else:
        # Якщо немає запиту, повертаємо всі документи (або обмежуємо)
        es_query["query"]["bool"]["must"].append({"match_all": {}})


    # TODO: Можна додати фільтри за параметрами запиту, наприклад:
    # if 'country_code' in request.args:
    #     es_query["query"]["bool"]["must"].append({"term": {"geolocation.country_code": request.args['country_code'].lower()}})
    # if 'service' in request.args:
    #     es_query["query"]["bool"]["must"].append({"term": {"service_name_inferred.keyword": request.args['service']}})


    try:
        response = await es_client_api.search(index=INDEX_NAME, body=es_query) # Використовуємо es_client_api
        
        hits = response['hits']['hits']
        total_hits = response['hits']['total']['value']

        results = [hit['_source'] for hit in hits]
        
        return jsonify({
            "results": results,
            "total": total_hits,
            "page": page,
            "size": size
        })
    except (ConnectionError, ApiError) as e:
        print(f"Помилка Elasticsearch API під час пошуку: {e}")
        return jsonify({"error": "Помилка сервера під час пошуку даних."}), 500
    except Exception as e:
        print(f"Непередбачена помилка під час пошуку: {e}")
        return jsonify({"error": "Невідома помилка сервера."}), 500

@app.route('/device/<ip_address>/<port>', methods=['GET'])
async def get_device_details(ip_address, port):
    if es_client_api is None:
        return jsonify({"error": "Elasticsearch недоступний. Будь ласка, запустіть його та перевірте підключення веб-інтерфейсу."}), 500

    device_id = f"{ip_address}-{port}" # Використовуємо той самий ID, що і при індексації
    try:
        response = await es_client_api.get(index=INDEX_NAME, id=device_id) # Використовуємо es_client_api
        if response['found']:
            return jsonify(response['_source'])
        else:
            return jsonify({"error": "Пристрій не знайдено."}), 404
    except NotFoundError:
        return jsonify({"error": "Пристрій не знайдено."}), 404
    except (ConnectionError, ApiError) as e:
        print(f"Помилка Elasticsearch API під час отримання деталей пристрою: {e}")
        return jsonify({"error": "Помилка сервера під час отримання деталей."}), 500
    except Exception as e:
        print(f"Непередбачена помилка під час отримання деталей: {e}")
        return jsonify({"error": "Невідома помилка сервера."}), 500

if __name__ == '__main__':
    # Змінено: Тепер надаємо інструкції для запуску через Hypercorn
    print("Для запуску веб-інтерфейсу (API) ScanEngine, виконайте в терміналі:")
    print("1. Переконайтеся, що ви активували віртуальне середовище.")
    print("2. Встановіть Hypercorn, якщо ще не встановлено: pip install hypercorn")
    print("3. Запустіть сервер: hypercorn src.api:app --bind 0.0.0.0:5000 --worker-class asyncio --workers 1")
    print("\nПісля запуску, перейдіть до http://127.0.0.1:5000/search?q=<ваш_запит>")
