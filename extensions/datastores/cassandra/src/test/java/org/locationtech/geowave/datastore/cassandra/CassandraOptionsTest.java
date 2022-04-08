/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
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
    final int size = mockOptions.getBatchWriteSize();
    Assert.assertEquals(batchWriteSize, size);
  }

  @Test
  public void testSetDurableWrites() {
    mockOptions.setDurableWrites(durableSize);
    final boolean isDurable = mockOptions.isDurableWrites();
    Assert.assertTrue(isDurable);
  }

  @Test
  public void testIsServerSideLibraryEnabled() {
    final boolean isServerEnabled = mockOptions.isServerSideLibraryEnabled();
    Assert.assertFalse(isServerEnabled);
  }

  @Test
  public void testSetReplicationFactor() {
    mockOptions.setReplicationFactor(replicationFactor);
    final int getRF = mockOptions.getReplicationFactor();
    Assert.assertEquals(replicationFactor, getRF);
  }
}
