# BSD Licensed, Copyright (c) 2006-2010 TileCache Contributors

from TileCache.Cache import Cache
import sys, os, time, warnings
from sqlite3 import connect, OperationalError, IntegrityError

_create_tables = """
    CREATE TABLE IF NOT EXISTS locks (
        row     INTEGER,
        column  INTEGER,
        zoom    INTEGER,
        format  TEXT,

        PRIMARY KEY (row, column, zoom, format)
    )
    """, """
    CREATE TABLE IF NOT EXISTS tiles (
        path    TEXT PRIMARY KEY,
        used    INTEGER,
        size    INTEGER
    )
    """, """
    CREATE INDEX IF NOT EXISTS tiles_used ON tiles (used)
    """


class LimitedDisk (Cache):
    def __init__ (self, base = None, umask = '002', limit = None, **kwargs):
        Cache.__init__(self, **kwargs)
        self.basedir = base
        self.umask = int(umask, 0)
        if sys.platform.startswith("java"):
            from java.io import File
            self.file_module = File
            self.platform = "jython"
        else:
            self.platform = "cpython"
        
        if not self.access(base, 'read'):
            self.makedirs(base)
        
        self.dbpath = os.path.join(base, 'cache.db')

        self.limit = None
        if limit is not None:
            self.limit = int(limit)

        #Create the database
        db = connect(self.dbpath).cursor()

        for create_table in _create_tables:
            db.execute(create_table)

        db.connection.close()



    def makedirs(self, path, hide_dir_exists=True):
        if hasattr(os, "umask"):
            old_umask = os.umask(self.umask)
        try:
            os.makedirs(path)
        except OSError, E:
            # os.makedirs can suffer a race condition because it doesn't check
            # that the directory  doesn't exist at each step, nor does it
            # catch errors. This lets 'directory exists' errors pass through,
            # since they mean that as far as we're concerned, os.makedirs
            # has 'worked'
            if E.errno != 17 or not hide_dir_exists:
                raise E
        if hasattr(os, "umask"):
            os.umask(old_umask)
        
    def access(self, path, type='read'):
        if self.platform == "jython":
            if type == "read":
                return self.file_module(path).canRead()
            else:
                return self.file_module(path).canWrite()
        else:
            if type =="read":
                return os.access(path, os.R_OK)
            else:
                return os.access(path, os.W_OK)

    def getKey (self, tile):
        components = ( self.basedir,
                       tile.layer.name,
                       "%02d" % tile.z,
                       "%03d" % int(tile.x / 1000000),
                       "%03d" % (int(tile.x / 1000) % 1000),
                       "%03d" % (int(tile.x) % 1000),
                       "%03d" % int(tile.y / 1000000),
                       "%03d" % (int(tile.y / 1000) % 1000),
                       "%03d.%s" % (int(tile.y) % 1000, tile.layer.extension)
                    )
        filename = os.path.join( *components )
        return filename

    def get (self, tile):
        filename = self.getKey(tile)
        if self.access(filename, 'read'):
            #File exists, so update the used column in the tiles
            db = connect(self.dbpath).cursor()
            db.execute("UPDATE tiles SET used=? WHERE path=?", (int(time.time()), filename))
            db.connection.commit()
            db.connection.close()

            if self.sendfile:
                return filename
            else:
                tile.data = file(filename, "rb").read()
                return tile.data
        else:
            return None

    def set (self, tile, data):
        if self.readonly: return data
        filename = self.getKey(tile)
        dirname  = os.path.dirname(filename)
        if not self.access(dirname, 'write'):
            self.makedirs(dirname)
        tmpfile = filename + ".%d.tmp" % os.getpid()
        if hasattr(os, "umask"):
            old_umask = os.umask(self.umask)
        output = file(tmpfile, "wb")
        output.write(data)
        output.close()
        if hasattr(os, "umask"):
            os.umask( old_umask );
        try:
            os.rename(tmpfile, filename)
        except OSError:
            os.unlink(filename)
            os.rename(tmpfile, filename)
        tile.data = data

        db = connect(self.dbpath).cursor()
        db.execute("""INSERT OR REPLACE INTO tiles (size, used, path) VALUES (?, ?, ?)""", (len(data), int(time.time()), filename))

        if self.limit is not None:
            row = db.execute('SELECT SUM(size) FROM tiles').fetchone()
            if row and (row[0] > self.limit):
                over = row[0] - self.limit
                while over > 0:
                    row = db.execute('SELECT path, size FROM tiles ORDER BY used ASC LIMIT 1').fetchone()
                    if row is None:
                        break

                    path, size = row
                    self._remove(path, db)
                    over -= size

            db.connection.commit()
            db.connection.close()

        return data
    
    def _remove (self, path, db=None):
        if self.access(path, 'read'):
            os.unlink(path)
        db.execute("DELETE FROM tiles WHERE path=?", (path, ))


    def delete (self, tile):
        filename = self.getKey(tile)
        db = connect(self.dbpath).cursor()
        self._remove(filename, db)
        db.connection.commit()
        db.connection.close()


    def attemptLock (self, tile):
        name = self.getLockName(tile)
        try: 
            self.makedirs(name, hide_dir_exists=False)
            return True
        except OSError:
            pass
        try:
            st = os.stat(name)
            if st.st_ctime + self.stale < time.time():
                warnings.warn("removing stale lock %s" % name)
                # remove stale lock
                self.unlock(tile)
                self.makedirs(name)
                return True
        except OSError:
            pass
        return False 
     
    def unlock (self, tile):
        name = self.getLockName(tile)
        try:
            os.rmdir(name)
        except OSError, E:
            print >>sys.stderr, "unlock %s failed: %s" % (name, str(E))
