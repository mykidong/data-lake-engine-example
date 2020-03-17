package mykidong.rocksdb;

import org.junit.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

public class PlayRocksDB {

    @Test
    public void handleRockDB() throws Exception
    {
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();

        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        try (final Options options = new Options().setCreateIfMissing(true)) {

            String dbPath = "/tmp/my-db";
            // a factory method that returns a RocksDB instance
            final RocksDB db = RocksDB.open(options, dbPath);

            byte[] key1 = "my-key1".getBytes();
            byte[] key2 = "my-key2".getBytes();

            byte[] value = db.get(key1);
            if (value != null) {
                System.out.println("value of key1: [" + value + "]");

                db.put(key2, value);

                // delete key1.
                db.delete(key1);
            }
            else
            {
                byte[] value2 = db.get(key2);
                if(value2 != null)
                {
                    System.out.println("value of key2: [" + value2 + "]");
                }

                value = "my-value".getBytes();
                db.put(key1, value);
            }

        } catch (RocksDBException e) {
        }
    }

}
