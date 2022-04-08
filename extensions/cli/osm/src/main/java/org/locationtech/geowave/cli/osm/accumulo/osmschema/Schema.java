/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.osm.accumulo.osmschema;

import org.apache.accumulo.core.data.ByteSequence;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class Schema {
  public static final ColumnFamily CF = new ColumnFamily();
  public static final ColumnQualifier CQ = new ColumnQualifier();
  protected static final HashFunction _hf = Hashing.murmur3_128(1);

  public static byte[] getIdHash(final long id) {
    return _hf.hashLong(id).asBytes();
  }

  public static boolean arraysEqual(final ByteSequence array, final byte[] value) {
    if (value.length != array.length()) {
      return false;
    }
    return startsWith(array, value);
  }

  public static boolean startsWith(final ByteSequence array, final byte[] prefix) {
    if (prefix.length > array.length()) {
      return false;
    }

    for (int i = 0; i < prefix.length; i++) {
      if (prefix[i] != array.byteAt(i)) {
        return false;
      }
    }
    return true;
  }
}
