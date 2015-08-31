import ZODB.BaseStorage
import ZODB.blob
import ZODB.interfaces
import ZODB.MappingStorage
import ZODB.POSException
import ZODB.utils
import zope.interface
from ZODB.utils import z64, p64, u64
import pycouchdb.exceptions
import logging
from uuid import uuid4
from base64 import decodestring
from base64 import encodestring
import tempfile

log = logging.getLogger('zodb_nosql')


class CouchdbStorage(object):

    zope.interface.implements(
        ZODB.interfaces.IStorage,
        ZODB.interfaces.IStorageIteration,
        ZODB.interfaces.IBlobStorage)

    def __init__(self, name, uri, database):
        self.name = name
        self._last_transaction = None
        self.server = pycouchdb.Server(uri)
        try:
            self.db = self.server.database(database)
        except pycouchdb.exceptions.NotFound:
            self.server.create(database)
            self.db = self.server.database(database)
        self._oid = z64
        self._rev_index = {}
        self._blob_paths = {}

    def close(self):
        pass

    def getName(self):
        return self.name

    def getSize(self):
        return 0

    def history(self, oid, size=1):
        """
        should be implemented
        """
        return []

    def isReadOnly(self):
        return False

    def lastTransaction(self):
        """Return the id of the last committed transaction.

        If no transactions have been committed, return a string of 8
        null (0) characters.
        """
        return z64

    def __len__(self):
        """The approximate number of objects in the storage

        This is used soley for informational purposes.
        """
        return 0

    def load(self, oid, version):
        """Load data for an object id

        The version argumement should always be an empty string. It
        exists soley for backward compatibility with older storage
        implementations.

        A data record and serial are returned.  The serial is a
        transaction identifier of the transaction that wrote the data
        record.

        A POSKeyError is raised if there is no record for the object id.
        """
        try:
            result = self.db.get(oid)
            return decodestring(result['pickle']), p64(result['serial'])
        except pycouchdb.exceptions.NotFound:
            raise ZODB.POSException.POSKeyError('Could not find oid %s' % oid)

    def loadBefore(self, oid, tid):
        """Load the object data written before a transaction id

        If there isn't data before the object before the given
        transaction, then None is returned, otherwise three values are
        returned:

        - The data record

        - The transaction id of the data record

        - The transaction id of the following revision, if any, or None.

        If the object id isn't in the storage, then POSKeyError is raised.
        """
        return None

    def loadSerial(self, oid, serial):
        """Load the object record for the give transaction id

        If a matching data record can be found, it is returned,
        otherwise, POSKeyError is raised.
        """
        import pdb; pdb.set_trace()

    def new_oid(self):
        """Allocate a new object id.

        The object id returned is reserved at least as long as the
        storage is opened.

        The return value is a string.
        """
        return str(uuid4())
        # last = self._oid
        # d = ord(last[-1])
        # if d < 255:  # fast path for the usual case
        #     last = last[:-1] + chr(d+1)
        # else:        # there's a carry out of the last byte
        #     last_as_long, = _structunpack(">Q", last)
        #     last = _structpack(">Q", last_as_long + 1)
        # self._oid = last
        # return last

    def pack(self, pack_time, referencesf):
        pass

    def registerDB(self, wrapper):
        pass

    def sortKey(self):
        return self.name

    def store(self, oid, serial, data, version, transaction):
        if serial:
            prev_tid_int = u64(serial)
        else:
            prev_tid_int = 0
        obj = {
            'pickle': encodestring(data),
            'version': version,
            'serial': prev_tid_int,
            '_id': oid,
            'type': 'object'
        }
        if oid in self._rev_index:
            obj['_rev'] = self._rev_index[oid]
        try:
            self._rev_index[oid] = self.db.save(obj)['_rev']
        except pycouchdb.exceptions.Conflict:
            # current current doc so we can grab revision
            # XXX wish we could override...
            current = self.db.get(oid)
            obj['_rev'] = current['_rev']
            self._rev_index[oid] = self.db.save(obj)['_rev']
        except:
            import pdb; pdb.set_trace()
            raise

    def tpc_abort(self, transaction):
        log.info('tpc abort')

    def tpc_begin(self, transaction):
        log.info('tpc begin')

    def tpc_finish(self, transaction, func=lambda tid: None):
        log.info('tpc finish')

    def tpc_vote(self, transaction):
        pass

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        """
        we are not caring about not being current here...
        just pass, ignore this.
        XXX is this dangereous?
        """
        pass

    def storeBlob(self, oid, serial, data, blobfilename, version, transaction):
        if serial:
            prev_tid_int = u64(serial)
        else:
            prev_tid_int = 0
        obj = {
            '_id': oid,
            'version': version,
            'serial': prev_tid_int,
            'pickle': encodestring(data)
        }
        if oid in self._rev_index:
            obj['_rev'] = self._rev_index[oid]
        else:
            # always needs to be an existing object, get/store first
            try:
                obj = self.db.get(oid)
                obj.update({
                    'version': version,
                    'serial': prev_tid_int,
                    'data': encodestring(data)
                })
            except pycouchdb.exceptions.NotFound:
                obj = self.db.save(obj)
                self._rev_index[oid] = obj['_rev']

        fi = open(blobfilename, 'rb')
        try:
            self._rev_index[oid] = self.db.put_attachment(obj, fi, 'data.blob')['_rev']
            fi.close()
        except pycouchdb.exceptions.Conflict:
            current = self.db.get(oid)
            obj['_rev'] = current['_rev']
            self._rev_index[oid] = self.db.put_attachment(obj, fi, 'data.blob')['_rev']
        finally:
            fi.close()

    def loadBlob(self, oid, serial):
        """Return the filename of the Blob data for this OID and serial.

        Returns a filename.

        Raises POSKeyError if the blobfile cannot be found.
        """
        obj = self.db.get(oid)
        if oid not in self._blob_paths:
            fiobj = self.db.get_attachment(obj, 'data.blob', stream=True)
            _, filepath = tempfile.mkstemp()
            fi = open(filepath, 'wb')
            fi.write(fiobj.raw.read())
            fi.close()
            self._blob_paths[oid] = filepath
        return self._blob_paths[oid]

    def openCommittedBlobFile(self, oid, serial, blob=None):
        """Return a file for committed data for the given object id and serial

        If a blob is provided, then a BlobFile object is returned,
        otherwise, an ordinary file is returned.  In either case, the
        file is opened for binary reading.

        This method is used to allow storages that cache blob data to
        make sure that data are available at least long enough for the
        file to be opened.
        """
        blob_filename = self.loadBlob(oid, serial)
        try:
            if blob is None:
                return open(blob_filename, 'rb')
            else:
                return ZODB.blob.BlobFile(blob_filename, 'r', blob)
        except (IOError):
            # The file got removed while we were opening.
            # Fall through and try again with the protection of the lock.
            pass

    def temporaryDirectory(self):
        """Return a directory that should be used for uncommitted blob data.

        If Blobs use this, then commits can be performed with a simple rename.
        """
        pass