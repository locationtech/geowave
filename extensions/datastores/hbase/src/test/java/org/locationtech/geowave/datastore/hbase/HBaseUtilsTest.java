package org.locationtech.geowave.datastore.hbase;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.Test;
import org.locationtech.geowave.datastore.hbase.util.HBaseUtils;

import java.io.IOException;

public class HBaseUtilsTest {

    @Test
    public void testGetQualifiedTableName() {
        String ret = HBaseUtils.getQualifiedTableName(null, "name2");
        assertEquals(ret, "name2");
        ret = HBaseUtils.getQualifiedTableName("name", "prefixnamesurfix");
        assertEquals(ret, "prefixnamesurfix");
        ret = HBaseUtils.getQualifiedTableName("name1", "name2");
        assertEquals(ret, "name1_name2");
    }

    @Test
    public void testWriteTableNameAsConfigSafe() {
        String ret = HBaseUtils.writeTableNameAsConfigSafe("a.b.c");
        assertEquals(ret, "a:b:c");
    }
}
