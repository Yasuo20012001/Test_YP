from flask import Flask, abort, request, jsonify
import elasticsearch as ES
# Согласно PEP8, импорты разделяются пустой строкой между каждой группой импортов.
# Импорты нужно отсортировать в правильном порядке:
# - импорты из стандартных библиотек Python
# - импорты сторонних библиотек (django, rest_framework и т.д.)
# - импорты модулей этого проекта
# Алфавитный порядок тоже является частью сортировки. 
# В VSCode есть встроенный сортировщик импортов, можно воспользоваться им или установить утилиту isort.
from validate import validate_args

app = Flask(__name__)

# Рекомендация: можно добавить конфигурацию приложения через переменные окружения для гибкости настройки.

@app.route('/')
def index():
    return 'worked'


@app.route('/api/movies/')
def movie_list():
    # Рекомендуется расширить валидацию, проверяя типы данных (например, limit и page должны быть числами)
    validate = validate_args(request.args)

    if not validate['success']:
        return abort(422)

    defaults = {
        'limit': 50,
        'page': 1,
        'sort': 'id',
        'sort_order': 'asc'
    }

    # Рекомендация: Проверьте, что все входные параметры имеют корректные типы.
    # Возможное улучшение: добавьте обработку исключений при приведении типов.
    for param in request.args.keys():
        defaults[param] = request.args.get(param)

    # Рекомендация: Расширьте поиск, добавив поля: "description", "genre", "actors_names", "writers_names" и "director".
    body = {
        "query": {
            "multi_match": {
                "query": defaults['search'],
                "fields": ["title"]
            }
        }
    } if defaults.get('search', False) else {}

    # Определяем, какие поля нужно включить в ответ.
    body['_source'] = dict()
    body['_source']['include'] = ['id', 'title', 'imdb_rating']

    # Рекомендуется проверять корректность значений сортировки и наличие поля для сортировки.
    params = {
        'from': int(defaults['limit']) * (int(defaults['page']) - 1),
        'size': defaults['limit'],
        'sort': [
            {
                defaults["sort"]: defaults["sort_order"]
            }
        ]
    }

    # Рекомендация: создание нового клиента для каждого запроса неэффективно.
    # Рассмотрите возможность использования единого клиента или менеджера контекста.
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )  # Длина строки превышает 79 символов (PEP 8)
    
    # Рекомендуется добавить обработку исключений для надежного выполнения запроса.
    search_res = es_client.search(
        body=body,
        index='movies',
        params=params,
        filter_path=['hits.hits._source']
    )
    es_client.close()

    return jsonify([doc['_source'] for doc in search_res['hits']['hits']])


@app.route('/api/movies/<string:movie_id>')
def get_movie(movie_id):
    # Рекомендация: оптимизируйте создание клиента, используя единый клиент или менеджер контекста.
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )  # Длина строки превышает 79 символов (PEP 8)

    # Рекомендуется использовать логирование вместо print для фиксации ошибок.
    if not es_client.ping():
        print('oh(')

    # Рекомендуется добавить обработку исключений для обработки ошибок запроса.
    search_result = es_client.get(index='movies', id=movie_id, ignore=404)

    es_client.close()

    if search_result['found']:
        return jsonify(search_result['_source'])

    return abort(404)

# Рекомендация: настройте параметры запуска (host, port) через переменные окружения или конфигурационный файл.
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
