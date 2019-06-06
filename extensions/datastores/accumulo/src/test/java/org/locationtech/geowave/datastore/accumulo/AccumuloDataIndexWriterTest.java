/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership. All rights reserved. This program and the accompanying materials are made available under the terms of the Apache License, Version 2.0 which accompanies this distribution and is available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo;

import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import java.io.IOException;
import java.util.Iterator;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloDataIndexWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AccumuloDataIndexWriterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloDataIndexWriterTest.class);

  final AccumuloOptions accumuloOptions = new AccumuloOptions();

  AccumuloOperations accumuloOperations;


  @Before
  public void setUp() {
    final MockInstance mockInstance = new MockInstance();
    Connector mockConnector = null;
    try {
      mockConnector = mockInstance.getConnector("root", new PasswordToken(new byte[0]));
    } catch (AccumuloException | AccumuloSecurityException e) {
      LOGGER.error("Failed to create mock accumulo connection", e);
    }
    accumuloOperations = new AccumuloOperations(mockConnector, accumuloOptions);
  }

  @Test
  public void testWrite() throws IOException, TableNotFoundException {

    // This is an adapter, that is needed to describe how to persist the
    // data type passed
    final Index index = DataIndexUtils.DATA_ID_INDEX;
    accumuloOperations.createTable(index.getName(), true, true);
    String tableName = index.getName();
    try (AccumuloDataIndexWriter dataIndexWriter =
        new AccumuloDataIndexWriter(
            accumuloOperations.createBatchWriter(tableName),
            accumuloOperations,
            tableName)) {
      dataIndexWriter.write(
          new GeoWaveRowImpl(
              new GeoWaveKeyImpl(new byte[] {2}, (short) 0, new byte[0], new byte[0], 0),
              new GeoWaveValue[] {new GeoWaveValueImpl(new byte[0], new byte[0], new byte[] {5})}));

      dataIndexWriter.write(
          new GeoWaveRowImpl(
              new GeoWaveKeyImpl(new byte[] {1}, (short) 0, new byte[0], new byte[0], 0),
              new GeoWaveValue[] {new GeoWaveValueImpl(new byte[0], new byte[0], new byte[] {7})}));
    }
    Iterator<GeoWaveRow> it =
        accumuloOperations.getDataIndexResults(new byte[][] {new byte[] {1}}, (short) 0);
    Assert.assertTrue(it.hasNext());
    GeoWaveRow row = it.next();
    Assert.assertTrue(row.getFieldValues()[0].getValue().length == 1);

    Assert.assertTrue(row.getFieldValues()[0].getValue()[0] == (byte) 7);
    Assert.assertFalse(it.hasNext());
  }

}
