package org.locationtech.geowave.datastore.hbase;

import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.datastore.hbase.util.ConnectionPool;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ConnectionPoolTest {
    ConnectionPool connectionPool;

    @Before
    public void setup() {
        this.connectionPool = ConnectionPool.getInstance();
    }

    @Test
    public void testGetInstance() {
        connectionPool = ConnectionPool.getInstance();
        ConnectionPool connectionPool2 = ConnectionPool.getInstance();
        assertEquals(connectionPool2, connectionPool);
    }

}