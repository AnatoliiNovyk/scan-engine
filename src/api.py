import asyncio
from flask import Flask, request, jsonify
from elasticsearch import AsyncElasticsearch, ConnectionError, NotFoundError, ApiError

app = Flask(__name__)

# Змінено: Змінна es тепер ініціалізується як None. 
# Клієнт буде створений асинхронно при першому запиті або в on_startup
es: AsyncElasticsearch = None 

INDEX_NAME = "scan_results"

# Функція для ініціалізації клієнта Elasticsearch для API
async def init_api_es_client():
    global es
    if es: # Якщо клієнт вже існує, закриваємо його, щоб уникнути Unclosed client session
        try:
            await es.close()
            print("Попередній клієнт Elasticsearch для API закрито.")
        except Exception as e:
            print(f"Помилка при закритті попереднього клієнта ES для API: {e}")

    try:
        es = AsyncElasticsearch([{'host': 'localhost', 'port': 9284, 'scheme': 'http'}])
        if await es.ping():
            print("Веб-інтерфейс: Успішно підключено до Elasticsearch.")
            return True
        else:
            print("Веб-інтерфейс: Попередження: Не вдалося підключитися до Elasticsearch.")
            es = None
            return False
    except ConnectionError as e:
        print(f"Веб-інтерфейс: Помилка підключення до Elasticsearch: {e}")
        es = None
        return False
    except Exception as e:
        print(f"Веб-інтерфейс: Критична помилка при ініціалізації клієнта Elasticsearch: {e}")
        es = None
        return False

# Функція для закриття клієнта Elasticsearch для API
async def close_api_es_client():
    global es
    if es:
        print("Веб-інтерфейс: Закриття клієнта Elasticsearch...")
        await es.close()
        print("Веб-інтерфейс: Клієнт Elasticsearch закрито.")
        es = None

# Додаємо обробники для життєвого циклу Flask (вимагає ASGI сервера для повного функціоналу)
@app.before_request
async def before_request_hook():
    global es
    if es is None:
        await init_api_es_client() # Спробувати ініціалізувати клієнт при першому запиті, якщо він не доступний

@app.teardown_request
async def teardown_request_hook(exception=None):
    pass # Клієнт ES не закриваємо після кожного запиту, закриємо при завершенні роботи додатку

@app.route('/')
async def home(): # Змінено: робимо функцію асинхронною
    return "Ласкаво просимо до ScanEngine! Використовуйте /search для пошуку."

@app.route('/search', methods=['GET'])
async def search_data():
    global es # Отримуємо доступ до глобальної змінної es
    if es is None:
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
        response = await es.search(index=INDEX_NAME, body=es_query)
        
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
    global es # Отримуємо доступ до глобальної змінної es
    if es is None:
        return jsonify({"error": "Elasticsearch недоступний. Будь ласка, запустіть його та перевірте підключення веб-інтерфейсу."}), 500

    device_id = f"{ip_address}-{port}" # Використовуємо той самий ID, що і при індексації
    try:
        response = await es.get(index=INDEX_NAME, id=device_id)
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

# Додаємо функцію для запуску з ASGI-сервером
async def run_api_server():
    # Запускаємо клієнт ES для API перед запуском сервера
    await init_api_es_client()

    # Запуск Flask-додатку за допомогою ASGI сервера (наприклад, Hypercorn)
    # Вам потрібно буде встановити 'hypercorn': pip install hypercorn
    # Потім запускати: hypercorn src.api:app --bind 0.0.0.0:5000
    print("Веб-сервер Flask готовий до запуску.")
    print("Для запуску виконайте (в окремому терміналі):")
    print("pip install hypercorn")
    print("hypercorn src.api:app --bind 0.0.0.0:5000 --worker-class asyncio --workers 1") # Використання --worker-class asyncio для асинхронних View Functions
    
# Цей блок залишаємо для локального тестування, але для продукції краще використовувати ASGI
if __name__ == '__main__':
    # Flask 2.x+ дозволяє асинхронні View Functions з вбудованим сервером
    print("Запуск веб-сервера Flask на http://127.0.0.1:5000/ (вбудований сервер)")
    print("Для пошуку використовуйте: http://127.0.0.1:5000/search?q=<ваш_запит>")
    print("Наприклад: http://127.0.0.1:5000/search?q=SSH")
    
    # Ініціалізуємо ES клієнт при запуску API
    asyncio.run(init_api_es_client())
    
    app.run(debug=True, port=5000)
    # Після завершення роботи веб-сервера, закриваємо клієнт ES
    # Це може не спрацювати автоматично для app.run()
    # Якщо потрібно явно закрити, додайте логіку завершення процесу
    asyncio.run(close_api_es_client())
