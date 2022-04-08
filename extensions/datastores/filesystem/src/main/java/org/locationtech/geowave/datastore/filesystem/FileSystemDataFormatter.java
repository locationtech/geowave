/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem;

import java.util.Arrays;
import java.util.Optional;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

public interface FileSystemDataFormatter {
  public static interface DataIndexFormatter {
    /**
     * Get the data ID for a given file name. The type name is also provided. This is the inverse of
     * getting the file name for a data ID.
     *
     * @param fileName the file name
     * @param typeName the type name
     * @return the data ID
     */
    byte[] getDataId(String fileName, String typeName);

    /**
     * Get the geowave value for a given file name and file contents. The type name is also
     * provided. This is the inverse of formatting the data.
     *
     * @param fileName the file name
     * @param typeName the type name
     * @param dataId the data ID
     * @param fileContents the contents of the file
     * @return the GeoWaveValue for the formatted data
     */
    GeoWaveValue getValue(String fileName, String typeName, byte[] dataId, byte[] fileContents);

    /**
     * When using secondary indexing, this is called for storing the data values
     *
     * @param typeName the DataTypeAdapter type name
     * @param dataId the data ID
     * @return the file name to use for this data ID
     */
    String getFileName(final String typeName, byte[] dataId);

    /**
     * When using secondary indexing, this is called for storing the data values
     *
     * @param typeName the DataTypeAdapter type name
     * @param dataId the data ID
     * @param value the value
     * @return the expected contents of the file according to this format
     */
    byte[] getFileContents(final String typeName, byte[] dataId, GeoWaveValue value);

    /**
     * Gives the formatter an opportunity to override the default directory for the data index with
     * this type name. Each data index *must* have a unique directory per type name for the system
     * to function properly. Java NIO is used to resolve this name relative to the filesystem data
     * store base directory, so if proper path separators are used for Java NIO to resolve nested
     * directories, they can be specified as well. GeoWave will ensure directories are created prior
     * to writing data
     *
     * @param typeName the type name
     * @return the directory name
     */
    default String getDirectoryName(final String typeName) {
      return typeName + "_" + DataIndexUtils.DATA_ID_INDEX.getName();
    }
  }

  public static interface IndexFormatter {


    /**
     * When using secondary indexing, this is called for storing the data values.
     *
     * @param fileName the file name
     * @param typeName the DataTypeAdapter type name
     * @param indexName the name of this index
     * @param expectsTime whether this index/type anticipates time is encoded in the file name
     *        (typically raster data only, and not with the secondary data index) *
     * @return an object containing a file name and the expected contents of the file according to
     *         this format
     */
    FileSystemIndexKey getKey(
        String fileName,
        String typeName,
        String indexName,
        boolean expectsTime);

    /**
     *
     * @param key the key (as resolved by this file reader's getKey() method)
     * @param fileInfo the file name and contents
     * @param typeName the DataTypeAdapter type name
     * @param indexName the name of this index
     * @return a GeoWaveValue to be used for reading the original data back into the system
     */
    GeoWaveValue getValue(
        FileSystemIndexKey key,
        String typeName,
        String indexName,
        FormattedFileInfo fileInfo);

    /**
     * This is called for storing values in each GeoWave index. Keep in mind that if the datastore
     * is using secondary indexing, the value provided to this method will be the data ID and not
     * the full data value. The full data value will be provided in the data index.
     *
     * @param typeName the DataTypeAdapter type name
     * @param indexName the name of this index
     * @param key the key for this row, including various GeoWave elements that are anticipated as
     *        components of a key
     * @param value the value
     * @return an object containing a file name and the expected contents of the file according to
     *         this format
     */
    FormattedFileInfo format(
        final String typeName,
        final String indexName,
        FileSystemIndexKey key,
        GeoWaveValue value);

    /**
     * Gives the formatter an opportunity to override the default directory for each index/type
     * pair. Each index *must* have a unique directory per type name for the system to function
     * properly. Java NIO is used to resolve this name relative to the filesystem data store base
     * directory, so if proper path separators are used for Java NIO to resolve nested directories,
     * they can be specified as well. GeoWave will ensure directories are created prior to writing
     * data.
     *
     * @param indexName the indexName name
     * @param typeName the type name
     * @return the directory name
     */
    default String getDirectoryName(final String indexName, final String typeName) {
      return typeName + "_" + indexName;
    }

    /**
     * Gives the formatter an opportunity to override the default directory for each partition (when
     * the index uses partitions). Keep in mind the index directory name is already implicitly in
     * the path.
     *
     * @param indexName the indexName name
     * @param typeName the type name
     * @param partitionKey the partition key
     * @return the partition's directory name
     */
    default String getPartitionDirectoryName(
        final String indexName,
        final String typeName,
        final byte[] partitionKey) {
      if ((partitionKey != null) && (partitionKey.length > 0)) {
        return ByteArrayUtils.byteArrayToString(partitionKey);
      }
      return "";
    }

    /**
     * This is the inverse of getPartiionDirectoryName so that partitions can be read from the
     * directory structure
     *
     * @param indexName the index name
     * @param typeName the type name
     * @param partitionDirectoryName the partition directory name that resolves from this partition
     *        key
     * @return the partition key represented by this directory
     */
    default byte[] getPartitionKey(
        final String indexName,
        final String typeName,
        final String partitionDirectoryName) {
      return ByteArrayUtils.byteArrayFromString(partitionDirectoryName);
    }

  }
  public static class FileSystemIndexKey {
    private final byte[] sortKey;
    private final byte[] dataId;
    // some data merging adapters expect to be able to write multiple duplicate sort/data ID keys
    // the results in the order
    private final Optional<Long> timeMillis;
    // at times there can be duplicates stored (such as a spatial temporal index that crosses time
    // periodicities) and this is a hint for query retrieval and deletion to expect duplicate
    private final short numDuplicates;

    public FileSystemIndexKey(
        final byte[] sortKey,
        final byte[] dataId,
        final Optional<Long> timeMillis,
        final short numDuplicates) {
      super();
      this.sortKey = sortKey;
      this.dataId = dataId;
      this.timeMillis = timeMillis;
      this.numDuplicates = numDuplicates;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + Arrays.hashCode(dataId);
      result = (prime * result) + numDuplicates;
      result = (prime * result) + Arrays.hashCode(sortKey);
      result = (prime * result) + ((timeMillis == null) ? 0 : timeMillis.hashCode());
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
      final FileSystemIndexKey other = (FileSystemIndexKey) obj;
      if (!Arrays.equals(dataId, other.dataId)) {
        return false;
      }
      if (numDuplicates != other.numDuplicates) {
        return false;
      }
      if (!Arrays.equals(sortKey, other.sortKey)) {
        return false;
      }
      if (timeMillis == null) {
        if (other.timeMillis != null) {
          return false;
        }
      } else if (!timeMillis.equals(other.timeMillis)) {
        return false;
      }
      return true;
    }

    public byte[] getSortKey() {
      return sortKey;
    }

    public byte[] getDataId() {
      return dataId;
    }

    public Optional<Long> getTimeMillis() {
      return timeMillis;
    }

    public short getNumDuplicates() {
      return numDuplicates;
    }

    public byte[] getSortOrderKey() {
      if (timeMillis.isPresent()) {
        return Bytes.concat(sortKey, dataId, Longs.toByteArray(timeMillis.get()));
      } else {
        return Bytes.concat(sortKey, dataId);
      }
    }
  }
  public static class FormattedFileInfo {
    // file name should just be the last part of the path (the file name only), not a full path to
    // the file
    private final String fileName;
    // the contents to write to the file, this should be formatted appropriately
    private final byte[] fileContents;

    public FormattedFileInfo(final String fileName, final byte[] fileContents) {
      super();
      this.fileName = fileName;
      this.fileContents = fileContents;
    }

    public String getFileName() {
      return fileName;
    }

    public byte[] getFileContents() {
      return fileContents;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + Arrays.hashCode(fileContents);
      result = (prime * result) + ((fileName == null) ? 0 : fileName.hashCode());
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
      final FormattedFileInfo other = (FormattedFileInfo) obj;
      if (!Arrays.equals(fileContents, other.fileContents)) {
        return false;
      }
      if (fileName == null) {
        if (other.fileName != null) {
          return false;
        }
      } else if (!fileName.equals(other.fileName)) {
        return false;
      }
      return true;
    }
  }

  DataIndexFormatter getDataIndexFormatter();

  IndexFormatter getIndexFormatter();

  default String getMetadataDirectory() {
    return "metadata";
  }

}
