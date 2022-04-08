/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class RocksDBUtilsTest {

  @Test
  public void testGetTableName() {
    final String nullPartitionKeyName = RocksDBUtils.getTableName("prefix-null", null);
    assertEquals("prefix-null", nullPartitionKeyName);

    final String emptyPartitionKeyName = RocksDBUtils.getTableName("prefix-empty", new byte[] {});
    assertEquals("prefix-empty", emptyPartitionKeyName);
  }

}
