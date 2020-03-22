import logging

import numpy as np
from peewee import Model, AutoField, TextField, DoubleField, DoesNotExist, IntegrityError
from playhouse.postgres_ext import PostgresqlExtDatabase, ArrayField


# Print SQL statements
logging.basicConfig(level=logging.DEBUG)


# Configure database
db = PostgresqlExtDatabase('app', user='postgres', password='password')


def get_table_name(cls):
    ''' Use the class name as the table name. '''
    return cls.__name__


class ImageVectorCacheError(Exception):
    pass


class ImageVectorBaseModel(Model):
    id = AutoField()
    url = TextField(unique=True)
    vector = ArrayField(DoubleField)

    class Meta:
        database = db
        table_function = get_table_name


class ImageVectorCache:
    ''' Cache image feature vectors for a specific customer project. '''

    def __init__(self, project_id):
        # Assuming project_id is unique across all customer projects
        self._project_id = project_id

    def create(self):
        ''' Create a new database table to store image vectors for this project. '''
        # Dynamically create a new peewee model class
        table_name = f'image_vector_project_{self._project_id}'
        self._model_ = type(table_name, (ImageVectorBaseModel, ), {})

        # Create database table if it doesn't exist yet
        self._model.create_table(safe=True)

    def store(self, image_url, vector):
        ''' Store the specified vector for the specified image_url. '''
        try:
            return self._model.create(url=image_url, vector=list(vector))
        except IntegrityError as e:
            raise ImageVectorCacheError(f'There is already an image with url "{image_url}"') from e

    def read(self, image_url):
        ''' Get the stored vector for the specified image_url. '''
        try:
            vector = self._model.get(self._model.url == image_url).vector
        except DoesNotExist as e:
            raise ImageVectorCacheError(f'There is no image with url "{image_url}"') from e

        return np.array(vector)

    @property
    def _model(self):
        try:
            return self._model_
        except AttributeError as e:
            raise ImageVectorCacheError('You must call `create` before using the cache!') from e


def main():
    with db:
        cache = ImageVectorCache(123)
        cache.create()

        # Will raise an error when run the second time
        x = cache.store('url.xyz.com', np.ones(100))
        print(x.id, x.url)
        print(cache.read('url.xyz.com'))


if __name__ == '__main__':
    main()
