import sqlite3
import json
# Согласно PEP8, импорты разделяются пустой строкой между каждой группой импортов.
# Импорты нужно отсортировать в правильном порядке:
# - импорты из стандартных библиотек Python
# - импорты сторонних библиотек (djando, rest_framework и т.д.)
# - импорты модулей этого проекта
# Алфавитный порядок тоже является частью сортировки. 
# В VSCode есть встроенный сортировщик импортов
# можно воспользоваться им или установить утилиту isort.
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# В PEP8 есть четкие рекомендации по оформлению пробелов между функциями:
# Две пустые строки между определениями функций и классов
def extract():
    """
    extract data from sql-db
    :return:
    """
    connection = sqlite3.connect("db.sqlite")
    
    # Наверняка это пилится в один sql - запрос, но мне как-то лениво)))
    # Лучше использовать один SQL-запрос для оптимизации
    # Рекомендация: Вместо использования подзапросов для GROUP_CONCAT рассмотрите вариант с JOIN-ами,
    # чтобы повысить читаемость и надёжность запроса, особенно при большом объёме данных.
    cursor.execute("""
        select id, imdb_rating, genre, title, plot, director,
        -- comma-separated actor_id's
        (
            select GROUP_CONCAT(actor_id) from
            (
                select actor_id
                from movie_actors
                where movie_id = movies.id
            )
        ),
        
        max(writer, writers)
        from movies
    """)

    # Рекомендация: Использование cursor.fetchall() может привести к высоким затратам памяти при больших объемах данных.
    # Рассмотрите вариант итеративной обработки данных.
    raw_data = cursor.fetchall()

    # Рекомендация: Проверьте корректность фильтрации актеров и сценаристов по "N/A".
    actors = {row[0]: row[1] for row in cursor.execute('select * from actors where name != "N/A"')}
    writers = {row[0]: row[1] for row in cursor.execute('select * from writers where name != "N/A"')}

    return actors, writers, raw_data


# В PEP8 есть четкие рекомендации по оформлению пробелов между функциями и классов:
# Две пустые строки между определениями функций и классов
def transform(__actors, __writers, __raw_data):
    """

    :param __actors:
    :param __writers:
    :param __raw_data:
    :return:
    """
    documents_list = []
    for movie_info in __raw_data:
        movie_id, imdb_rating, genre, title, description, director, raw_actors, raw_writers = movie_info

        # Рекомендация: Используйте try/except для обработки JSON, если строка не соответствует формату.
        if raw_writers[0] == '[':
            parsed = json.loads(raw_writers)
            new_writers = ','.join([writer_row['id'] for writer_row in parsed])
        else:
            new_writers = raw_writers

        # Рекомендация: Убедитесь, что все значения raw_actors можно корректно преобразовать в int.
        writers_list = [(writer_id, __writers.get(writer_id)) for writer_id in new_writers.split(',')]
        actors_list = [(actor_id, __actors.get(int(actor_id))) for actor_id in raw_actors.split(',')]

        document = {
            "_index": "movies",
            "_id": movie_id,
            "id": movie_id,
            "imdb_rating": imdb_rating,
            "genre": genre.split(', '),
            "title": title,
            "description": description,
            "director": director,
            "actors": [
                {
                    "id": actor[0],
                    "name": actor[1]
                }
                for actor in set(actors_list) if actor[1]
            ],
            "writers": [
                {
                    "id": writer[0],
                    "name": writer[1]
                }
                for writer in set(writers_list) if writer[1]
            ]
        }

        # Рекомендация: Логируйте случаи с 'N/A' значениями.
        for key in document.keys():
            if document[key] == 'N/A':
                print('hehe') # Лучше сделать логирование через logging
                document[key] = None

        # Рекомендация: Проверьте корректность формирования полей actors_names и writers_names.
        document['actors_names'] = ", ".join([actor["name"] for actor in document['actors'] if actor]) or None
        document['writers_names'] = ", ".join([writer["name"] for writer in document['writers'] if writer]) or None

        import pprint
        pprint.pprint(document)

        documents_list.append(document)

    return documents_list

# В PEP8 есть четкие рекомендации по оформлению пробелов между определениями функций и классов:
# Две пустые строки между определениями функций и классов
def load(acts):
    """

    :param acts:
    :return:
    """
    # Рекомендация: Вынесите параметры подключения в переменные окружения.
    es = Elasticsearch([{'host': '192.168.1.252', 'port': 9200}])
    bulk(es, acts)

    return True

if __name__ == '__main__':
    load(transform(*extract()))
