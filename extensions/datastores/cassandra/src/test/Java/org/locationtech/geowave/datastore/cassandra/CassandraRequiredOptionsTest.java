package org.locationtech.geowave.datastore.cassandra;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.datastore.cassandra.config.CassandraRequiredOptions;

public class CassandraRequiredOptionsTest {
  private CassandraRequiredOptions mockRequiredOptions;
  final private String contactPoint = "contactPointTest";

  @Before
  public void setup() {
    mockRequiredOptions = new CassandraRequiredOptions();
  }

  @Test
  public void testSetContactPoint() {
    mockRequiredOptions.setContactPoint(contactPoint);
    String getCT = mockRequiredOptions.getContactPoint();
    Assert.assertEquals(contactPoint, getCT);
  }
}
