/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class models all of the necessary information for persisting data in the data store
 * (following the common index model) and is used internally within GeoWave as an intermediary
 * object between the direct storage format and the native data format. It is the responsibility of
 * the data adapter to convert to and from this object and the native object. It does not contain
 * any information about the entry in a particular index and is used when writing an entry, prior to
 * its existence in an index.
 */
public class PersistenceEncoding<T> {
  private Short internalAdapterId;
  private final byte[] dataId;
  protected final PersistentDataset<T> commonData;
  protected final PersistentDataset<byte[]> unknownData;
  protected static final Logger LOGGER = LoggerFactory.getLogger(PersistenceEncoding.class);
  protected static final double DOUBLE_TOLERANCE = 1E-12d;

  public PersistenceEncoding(
      final Short internalAdapterId,
      final byte[] dataId,
      final PersistentDataset<T> commonData,
      final PersistentDataset<byte[]> unknownData) {
    this.internalAdapterId = internalAdapterId;
    this.dataId = dataId;
    this.commonData = commonData;
    this.unknownData = unknownData;
  }

  public short getInternalAdapterId() {
    return internalAdapterId;
  }

  public void setInternalAdapterId(final short internalAdapterId) {
    this.internalAdapterId = internalAdapterId;
  }

  /**
   * Return the data that has been persisted but not identified by a field reader
   *
   * @return the unknown data that is yet to be identified by a field reader
   */
  public PersistentDataset<byte[]> getUnknownData() {
    return unknownData;
  }

  /**
   * Return the common index data that has been persisted
   *
   * @return the common index data
   */
  public PersistentDataset<T> getCommonData() {
    return commonData;
  }

  /**
   * Return the data ID, data ID's should be unique per adapter
   *
   * @return the data ID
   */
  public byte[] getDataId() {
    return dataId;
  }

  public boolean isDeduplicationEnabled() {
    return true;
  }
}
