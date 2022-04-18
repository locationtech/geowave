/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data;

/**
 * This class models all of the necessary information for persisting data in the data store
 * (following the common index model) and is used internally within GeoWave as an intermediary
 * object between the direct storage format and the native data format. It also contains information
 * about the persisted object within a particular index such as the insertion ID in the index and
 * the number of duplicates for this entry in the index, and is used when reading data from the
 * index.
 */
public class IndexedPersistenceEncoding<T> extends PersistenceEncoding<T> {
  private final byte[] insertionPartitionKey;
  private final byte[] insertionSortKey;
  private final int duplicateCount;

  public IndexedPersistenceEncoding(
      final Short internalAdapterId,
      final byte[] dataId,
      final byte[] insertionPartitionKey,
      final byte[] insertionSortKey,
      final int duplicateCount,
      final PersistentDataset<T> commonData,
      final PersistentDataset<byte[]> unknownData) {
    super(internalAdapterId, dataId, commonData, unknownData);
    this.insertionPartitionKey = insertionPartitionKey;
    this.insertionSortKey = insertionSortKey;
    this.duplicateCount = duplicateCount;
  }

  public boolean isAsync() {
    return false;
  }

  /**
   * Return the partition key portion of the insertion ID
   *
   * @return the insertion partition key
   */
  public byte[] getInsertionPartitionKey() {
    return insertionPartitionKey;
  }

  /**
   * Return the sort key portion of the insertion ID
   *
   * @return the insertion sort key
   */
  public byte[] getInsertionSortKey() {
    return insertionSortKey;
  }

  @Override
  public boolean isDeduplicationEnabled() {
    return duplicateCount >= 0;
  }

  /**
   * Return the number of duplicates for this entry. Entries are duplicated when a single row ID is
   * insufficient to index it.
   *
   * @return the number of duplicates
   */
  public int getDuplicateCount() {
    return duplicateCount;
  }

  /**
   * Return a flag indicating if the entry has any duplicates
   *
   * @return is it duplicated?
   */
  public boolean isDuplicated() {
    return duplicateCount > 0;
  }
}
