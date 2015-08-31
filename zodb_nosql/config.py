from ZODB.config import BaseConfig
from zodb_nosql.couchdb import CouchdbStorage


class NoSQLFactory(BaseConfig):

    def open(self):
        if self.config.uri.startswith('couchdb://'):
            uri = self.config.uri[len('couchdb://'):]
            return CouchdbStorage(
                self.config.name or 'zodb_main', uri, self.config.database)
