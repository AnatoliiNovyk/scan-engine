import asyncio
from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch, ConnectionError, NotFoundError, ApiError

app = Flask(__name__)

# Настройка соединения с Elasticsearch
# Вам нужно будет раскомментировать это и, возможно, настроить аутентификацию
# Если вы отключили безопасность в elasticsearch.yml, то этого может быть достаточно:
try:
    es = Elasticsearch([{'host': 'localhost', 'port': 9284, 'scheme': 'http'}])
    # Проверка соединения
    if not es.ping():
        print("Предупреждение: Не удалось подключиться к Elasticsearch. Проверьте, запущен ли он на http://localhost:9284")
        es = None
    else:
        print("Веб-интерфейс: Успешно подключено к Elasticsearch.")
except ConnectionError as e:
    print(f"Веб-интерфейс: Ошибка подключения к Elasticsearch: {e}")
    es = None # Установить es в None, если подключение не удалось

INDEX_NAME = "scan_results"

@app.route('/')
def home():
    return "Добро пожаловать в ScanEngine! Используйте /search для поиска."

@app.route('/search', methods=['GET'])
async def search_data():
    if es is None:
        return jsonify({"error": "Elasticsearch недоступен. Пожалуйста, запустите его."}), 500

    query_string = request.args.get('q', '') # Поисковый запрос
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
        # Если нет запроса, возвращаем все документы (или ограничиваем)
        es_query["query"]["bool"]["must"].append({"match_all": {}})


    # TODO: Можно добавить фильтры по параметрам запроса, например:
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
        print(f"Ошибка Elasticsearch API при поиске: {e}")
        return jsonify({"error": "Ошибка сервера при поиске данных."}), 500
    except Exception as e:
        print(f"Непредвиденная ошибка при поиске: {e}")
