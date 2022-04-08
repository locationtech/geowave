/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.DataIndexFormatter;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.FileSystemIndexKey;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.FormattedFileInfo;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.IndexFormatter;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatterSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

public class GeoWaveBinaryDataFormatter implements FileSystemDataFormatterSpi {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveBinaryDataFormatter.class);
  public static final String DEFAULT_BINARY_FORMATTER = "binary";

  @Override
  public String getFormatName() {
    return DEFAULT_BINARY_FORMATTER;
  }

  @Override
  public String getFormatDescription() {
    return "A compact geowave serialization, used as default.";
  }

  @Override
  public FileSystemDataFormatter createFormatter(final boolean visibilityEnabled) {
    return new BinaryFormatter(visibilityEnabled);
  }

  private static class BinaryFormatter implements FileSystemDataFormatter {

    private final DataIndexFormatter dataIndexFormatter;
    private final IndexFormatter indexFormatter;

    private BinaryFormatter(final boolean visibilityEnabled) {
      dataIndexFormatter = new BinaryDataIndexFormatter(visibilityEnabled);
      indexFormatter = new BinaryIndexFormatter(visibilityEnabled);
    }

    @Override
    public DataIndexFormatter getDataIndexFormatter() {
      return dataIndexFormatter;
    }

    @Override
    public IndexFormatter getIndexFormatter() {
      return indexFormatter;
    }
  }

  private static class BinaryDataIndexFormatter implements DataIndexFormatter {
    private final boolean visibilityEnabled;

    private BinaryDataIndexFormatter(final boolean visibilityEnabled) {
      super();
      this.visibilityEnabled = visibilityEnabled;
    }

    @Override
    public String getFileName(final String typeName, final byte[] dataId) {
      return FileSystemUtils.keyToFileName(dataId);
    }

    @Override
    public byte[] getFileContents(
        final String typeName,
        final byte[] dataId,
        final GeoWaveValue value) {
      return DataIndexUtils.serializeDataIndexValue(value, visibilityEnabled);
    }

    @Override
    public byte[] getDataId(final String fileName, final String typeName) {
      return FileSystemUtils.fileNameToKey(fileName);
    }

    @Override
    public GeoWaveValue getValue(
        final String fileName,
        final String typeName,
        final byte[] dataId,
        final byte[] fileContents) {
      return DataIndexUtils.deserializeDataIndexValue(fileContents, visibilityEnabled);
    }


  }

  private static class BinaryFileSystemIndexKey extends FileSystemIndexKey {
    private final byte[] fieldMask;
    private final byte[] visibility;

    public BinaryFileSystemIndexKey(
        final byte[] sortKey,
        final byte[] dataId,
        final Optional<Long> timeMillis,
        final short numDuplicates,
        final byte[] fieldMask,
        final byte[] visibility) {
      super(sortKey, dataId, timeMillis, numDuplicates);
      this.fieldMask = fieldMask;
      this.visibility = visibility;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = (prime * result) + Arrays.hashCode(fieldMask);
      result = (prime * result) + Arrays.hashCode(visibility);
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!super.equals(obj)) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final BinaryFileSystemIndexKey other = (BinaryFileSystemIndexKey) obj;
      if (!Arrays.equals(fieldMask, other.fieldMask)) {
        return false;
      }
      if (!Arrays.equals(visibility, other.visibility)) {
        return false;
      }
      return true;
    }
  }

  private static class BinaryIndexFormatter implements IndexFormatter {
    private final boolean visibilityEnabled;
    private long prevTime = Long.MAX_VALUE;

    private BinaryIndexFormatter(final boolean visibilityEnabled) {
      super();
      this.visibilityEnabled = visibilityEnabled;
    }

    @Override
    public FileSystemIndexKey getKey(
        final String fileName,
        final String typeName,
        final String indexName,
        final boolean expectsTime) {
      int otherBytes = 4;
      final byte[] key = FileSystemUtils.fileNameToKey(fileName);
      final ByteBuffer buf = ByteBuffer.wrap(key);
      final byte[] sortKey = new byte[key[key.length - 2]];
      buf.get(sortKey);
      final byte[] fieldMask = new byte[key[key.length - 1]];
      final byte[] visibility;
      if (visibilityEnabled) {
        visibility = new byte[key[key.length - 3]];
        otherBytes++;
      } else {
        visibility = new byte[0];
      }
      if (expectsTime) {
        otherBytes += 8;
      }
      final byte[] dataId =
          new byte[key.length - otherBytes - sortKey.length - fieldMask.length - visibility.length];
      buf.get(dataId);
      Optional<Long> timeMillis;
      if (expectsTime) {
        // just skip 8 bytes - we don't care to parse out the timestamp but
        // its there for key uniqueness and to maintain expected sort order
        timeMillis = Optional.of(buf.getLong());
      } else {
        timeMillis = Optional.empty();
      }
      buf.get(fieldMask);
      if (visibilityEnabled) {
        buf.get(visibility);
      }
      final byte[] duplicatesBytes = new byte[2];
      buf.get(duplicatesBytes);
      final short duplicates = ByteArrayUtils.byteArrayToShort(duplicatesBytes);
      return new BinaryFileSystemIndexKey(
          sortKey,
          dataId,
          timeMillis,
          duplicates,
          fieldMask,
          visibility);
    }

    @Override
    public GeoWaveValue getValue(
        final FileSystemIndexKey key,
        final String typeName,
        final String indexName,
        final FormattedFileInfo fileInfo) {
      if (key instanceof BinaryFileSystemIndexKey) {
        return new GeoWaveValueImpl(
            ((BinaryFileSystemIndexKey) key).fieldMask,
            ((BinaryFileSystemIndexKey) key).visibility,
            fileInfo.getFileContents());
      } else if (key != null) {
        LOGGER.error(
            "Expected key not of type 'BinaryFileSystemIndexKey' not of type '"
                + key.getClass()
                + "'");
      } else {
        LOGGER.error("Unexpected null key");
      }
      return null;
    }

    @Override
    public FormattedFileInfo format(
        final String typeName,
        final String indexName,
        final FileSystemIndexKey key,
        final GeoWaveValue value) {
      byte[] binaryKey;
      byte[] endBytes;
      if (visibilityEnabled) {
        endBytes =
            Bytes.concat(
                value.getVisibility(),
                ByteArrayUtils.shortToByteArray(key.getNumDuplicates()),
                new byte[] {
                    (byte) value.getVisibility().length,
                    (byte) key.getSortKey().length,
                    (byte) value.getFieldMask().length});
      } else {
        endBytes =
            Bytes.concat(
                ByteArrayUtils.shortToByteArray(key.getNumDuplicates()),
                new byte[] {(byte) key.getSortKey().length, (byte) value.getFieldMask().length});
      }
      if (key.getTimeMillis().isPresent()) {
        // sometimes rows can be written so quickly that they are the exact
        // same millisecond - while Java does offer nanosecond precision,
        // support is OS-dependent. Instead this check is done to ensure
        // subsequent millis are written at least within this ingest
        // process.
        long time = Long.MAX_VALUE - System.currentTimeMillis();
        if (time >= prevTime) {
          time = prevTime - 1;
        }
        prevTime = time;
        binaryKey =
            Bytes.concat(
                key.getSortKey(),
                key.getDataId(),
                Longs.toByteArray(time),
                value.getFieldMask(),
                endBytes);
      } else {
        binaryKey = Bytes.concat(key.getSortKey(), key.getDataId(), value.getFieldMask(), endBytes);
      }
      return new FormattedFileInfo(FileSystemUtils.keyToFileName(binaryKey), value.getValue());
    }

  }
}
