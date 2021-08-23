import asyncio
import os
import functools
import json
import codecs
import types

import mongomock
from mongomock.results import DeleteResult
from mongomock import helpers
try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping
import pytest
import yaml
from bson import json_util


_cache = {}


def pytest_addoption(parser):

    parser.addini(
        name='async_mongodb_fixtures',
        help='Load these fixtures for tests',
        type='linelist')

    parser.addini(
        name='async_mongodb_fixture_dir',
        help='Try loading fixtures from this directory',
        default=os.getcwd())

    parser.addoption(
        '--async_mongodb-fixture-dir',
        help='Try loading fixtures from this directory')



def wrapper(func):
    @functools.wraps(func)
    async def wrapped(*args, **kwargs):
        coro_func = asyncio.coroutine(func)
        return await coro_func(*args, **kwargs)
    return wrapped


class AsyncClassMethod(object):

    ASYNC_METHODS = []

    def __getattribute__(self, name):
        attr = super(AsyncClassMethod, self).__getattribute__(name)
        if type(attr) == types.MethodType and name in self.ASYNC_METHODS:
            attr = wrapper(attr)
        return attr


class AsyncCollection(AsyncClassMethod, mongomock.Collection):

    ASYNC_METHODS = [
        'find_one',
        'find',
        'count',
        'insert_one',
        'update_one',
        '_delete'
    ]

    async def find_one(self, filter=None, *args, **kwargs):

        # Allow calling find_one with a non-dict argument that gets used as
        # the id for the query.
        if filter is None:
            filter = {}
        if not isinstance(filter, Mapping):
            filter = {'_id': filter}

        cursor = await self.find(filter, *args, **kwargs)
        try:
            return next(cursor)
        except StopIteration:
            return None
    
    async def delete_one(self, filter, collation=None, hint=None, session=None):
        result = await self._delete(filter, collation=collation, hint=hint, session=session)
        return DeleteResult(result, True)

    async def _delete(self, filter, collation=None, hint=None, multi=False, session=None):
        if hint:
            raise NotImplementedError(
                'The hint argument of delete is valid but has not been implemented in '
                'mongomock yet')
        if collation:
            raise NotImplementedError(
                'collation',
                'The collation argument of delete is valid but has not been '
                'implemented in mongomock yet')
        if session:
            NotImplementedError('session', 'Mongomock does not handle sessions yet')
        filter = helpers.patch_datetime_awareness_in_document(filter)
        if filter is None:
            filter = {}
        if not isinstance(filter, Mapping):
            filter = {'_id': filter}

        to_delete = list(await self.find(filter))

        deleted_count = 0
        for doc in to_delete:
            doc_id = doc['_id']
            if isinstance(doc_id, dict):
                doc_id = helpers.hashdict(doc_id)
            del self._store[doc_id]
            deleted_count += 1
            if not multi:
                break

        return {
            'connectionId': self.database.client._id,
            'n': deleted_count,
            'ok': 1.0,
            'err': None,
        }


class AsyncDatabase(AsyncClassMethod, mongomock.Database):

    def __init__(self, client, name):
        self._collections = {}
        super().__init__(client, name, None)
        


    ASYNC_METHODS = [
        'collection_names'
    ]

    def get_collection(self, name, codec_options=None, read_preference=None,
                       write_concern=None):
        collection = self._collections.get(name)
        if collection is None:
            collection = self._collections[name] = AsyncCollection(self, name, self._store)
        return collection


class AsyncMockMongoClient(mongomock.MongoClient):
    def __init__(self):
        self._databases = {}
        pass
    def get_database(self, name, codec_options=None, read_preference=None,
                     write_concern=None):
    
        db = self._databases.get(name)
        if db is None:
            db = self._databases[name] = AsyncDatabase(self, name)
        return db


@pytest.fixture(scope='function')
async def async_mongodb(pytestconfig, request):
    marker = request.node.get_closest_marker("collections")
    not_drop_exists = request.node.get_closest_marker("not_drop_exists")
    if marker is None:
        data = None
    else:
        v = marker.args[0]
        if isinstance(v, list):
            data = set(list(v))
        else:
            data = v
    client = AsyncMockMongoClient()
    db = client['pytest']
    if not_drop_exists is None:
        await clean_database(db)
    await load_fixtures(db, pytestconfig, data)
    return db


async def clean_database(db):
    collections = db.list_collection_names()
    for name in collections:
        db.drop_collection(name)


async def load_fixtures(db, config, collections):
    if collections is None:
        return
    option_dir = config.getoption('async_mongodb_fixture_dir')
    ini_dir = config.getini('async_mongodb_fixture_dir')
    fixtures = collections
    basedir = option_dir or ini_dir
    
    for file_name in os.listdir(basedir):
        collection, ext = os.path.splitext(os.path.basename(file_name))
        file_format = ext.strip('.')
        supported = file_format in ('json', 'yaml')
        selected = fixtures and collection in fixtures
        if selected and supported:
            path = os.path.join(basedir, file_name)
            if collection not in db.list_collection_names():
                await load_fixture(db, collection, path, file_format)


async def load_fixture(db, collection, path, file_format):
    
    if file_format == 'json':
        loader = functools.partial(json.load, object_hook=json_util.object_hook)
    elif file_format == 'yaml':
        loader = yaml.load
    else:
        return
    try:
        docs = _cache[path]
    except KeyError:
        with codecs.open(path, encoding='utf-8') as fp:
            s =loader(fp)
            _cache[path] = docs = s

    for document in docs:
        await db[collection].insert_one(document)
