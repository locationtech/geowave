package org.locationtech.geowave.datastore.cassandra;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.datastore.cassandra.config.CassandraOptions;

public class CassandraOptionsTest {

    final private int batchWriteSize = 10;
    private CassandraOptions mockOptions;
    final private boolean durableSize = true;
    final private int replicationFactor = 20;

    @Before
    public void setup() {
        mockOptions = new CassandraOptions();
    }

    @After
    public void cleanup() {
        mockOptions = null;
    }

    @Test
    public void testSetBatchWriteSize() {
        mockOptions.setBatchWriteSize(batchWriteSize);
        int size = mockOptions.getBatchWriteSize();
        Assert.assertEquals(batchWriteSize, size);
    }

    @Test
    public void testSetDurableWrites() {
        mockOptions.setDurableWrites(durableSize);
        boolean isDurable = mockOptions.isDurableWrites();
        Assert.assertTrue(isDurable);
    }

    @Test
    public void testIsServerSideLibraryEnabled() {
        boolean isServerEnabled = mockOptions.isServerSideLibraryEnabled();
        Assert.assertFalse(isServerEnabled);
    }

    @Test
    public void testSetReplicationFactor() {
        mockOptions.setReplicationFactor(replicationFactor);
        int getRF = mockOptions.getReplicationFactor();
        Assert.assertEquals(replicationFactor, getRF);
    }
}
