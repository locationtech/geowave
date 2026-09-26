/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class AbstractRocksDBIteratorTest {
  @Test
  public void testReadErrorFailsTheScan() throws RocksDBException {
    final RocksIterator it = mock(RocksIterator.class);
    when(it.isValid()).thenReturn(false);
    final RocksDBException error = new RocksDBException("Corruption: block checksum mismatch");
    doThrow(error).when(it).status();
    assertSame(error, assertThrows(IllegalStateException.class, rows(it)::hasNext).getCause());
  }

  @Test
  public void testEndOfDataEndsTheScan() throws RocksDBException {
    final RocksIterator it = mock(RocksIterator.class);
    when(it.isValid()).thenReturn(false);
    assertFalse(rows(it).hasNext());
    verify(it).status();
  }

  private static RocksDBRowIterator rows(final RocksIterator it) {
    return new RocksDBRowIterator(null, it, (short) 0, null, false, false);
  }
}
