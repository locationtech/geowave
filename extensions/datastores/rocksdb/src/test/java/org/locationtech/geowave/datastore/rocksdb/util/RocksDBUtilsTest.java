package org.locationtech.geowave.datastore.rocksdb.util;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class RocksDBUtilsTest {

  @Test
  public void testGetTableName() {
    String nullPartitionKeyName = RocksDBUtils.getTableName("prefix-null", null);
    assertEquals("prefix-null", nullPartitionKeyName);

    String emptyPartitionKeyName = RocksDBUtils.getTableName("prefix-empty", new byte[] {});
    assertEquals("prefix-empty", emptyPartitionKeyName);
  }

}
