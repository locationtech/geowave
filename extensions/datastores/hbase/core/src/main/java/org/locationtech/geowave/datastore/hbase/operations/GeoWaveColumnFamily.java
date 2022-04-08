/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.operations;

import java.util.Arrays;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public interface GeoWaveColumnFamily {
  public ColumnFamilyDescriptorBuilder toColumnDescriptor();

  public static interface GeoWaveColumnFamilyFactory {
    public GeoWaveColumnFamily fromColumnDescriptor(ColumnFamilyDescriptor column);
  }

  public static class StringColumnFamily implements GeoWaveColumnFamily {
    private final String columnFamily;

    public StringColumnFamily(final String columnFamily) {
      this.columnFamily = columnFamily;
    }

    @Override
    public ColumnFamilyDescriptorBuilder toColumnDescriptor() {
      return ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((columnFamily == null) ? 0 : columnFamily.hashCode());
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final StringColumnFamily other = (StringColumnFamily) obj;
      if (columnFamily == null) {
        if (other.columnFamily != null) {
          return false;
        }
      } else if (!columnFamily.equals(other.columnFamily)) {
        return false;
      }
      return true;
    }
  }

  public static class StringColumnFamilyFactory implements GeoWaveColumnFamilyFactory {
    public static StringColumnFamilyFactory getSingletonInstance() {
      return SINGLETON_INSTANCE;
    }

    private static final StringColumnFamilyFactory SINGLETON_INSTANCE =
        new StringColumnFamilyFactory();

    private StringColumnFamilyFactory() {}

    @Override
    public GeoWaveColumnFamily fromColumnDescriptor(final ColumnFamilyDescriptor column) {

      return new StringColumnFamily(column.getNameAsString());
    }
  }

  public static class ByteArrayColumnFamily implements GeoWaveColumnFamily {
    private final byte[] columnFamily;

    public ByteArrayColumnFamily(final byte[] columnFamily) {
      this.columnFamily = columnFamily;
    }

    @Override
    public ColumnFamilyDescriptorBuilder toColumnDescriptor() {
      return ColumnFamilyDescriptorBuilder.newBuilder(columnFamily);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + Arrays.hashCode(columnFamily);
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final ByteArrayColumnFamily other = (ByteArrayColumnFamily) obj;
      if (!Arrays.equals(columnFamily, other.columnFamily)) {
        return false;
      }
      return true;
    }
  }

  public static class ByteArrayColumnFamilyFactory implements GeoWaveColumnFamilyFactory {
    public static ByteArrayColumnFamilyFactory getSingletonInstance() {
      return SINGLETON_INSTANCE;
    }

    private static final ByteArrayColumnFamilyFactory SINGLETON_INSTANCE =
        new ByteArrayColumnFamilyFactory();

    private ByteArrayColumnFamilyFactory() {}

    @Override
    public GeoWaveColumnFamily fromColumnDescriptor(final ColumnFamilyDescriptor column) {
      return new ByteArrayColumnFamily(column.getName());
    }
  }
}
