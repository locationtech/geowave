package org.locationtech.geowave.datastore.cassandra;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import com.datastax.driver.core.Row;

public class CassandraRowTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRowTest.class);
    private Row row;
    private CassandraRow cassandraRow;

    @Before
    public void setup() {
        cassandraRow = new CassandraRow(row);
    }


    @Test
    public void testGetDataId() {
        byte[] isDataIndexColumn = cassandraRow.getDataId();
        Assert.assertNotNull(isDataIndexColumn);
    }

    @Test
    public void testSortKey() {
        byte[] sortKey = cassandraRow.getSortKey();
        Assert.assertNotNull(sortKey);
    }

    @Test
    public void testGetPartitionKey() {
        byte[] partitionKey = cassandraRow.getPartitionKey();
        Assert.assertNotNull(partitionKey);
    }
}
