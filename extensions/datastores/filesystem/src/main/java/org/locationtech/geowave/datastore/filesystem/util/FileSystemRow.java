/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.MergeableGeoWaveRow;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.FileSystemIndexKey;

public class FileSystemRow extends MergeableGeoWaveRow implements GeoWaveRow {
  List<String> mergedFileNames;
  private final String fileName;
  private final short adapterId;
  private final byte[] partition;
  private final byte[] sortKey;
  private final byte[] dataId;
  private final short duplicates;

  public FileSystemRow(
      final String fileName,
      final short adapterId,
      final byte[] partition,
      final FileSystemIndexKey key,
      final GeoWaveValue value) {
    super();
    this.fileName = fileName;
    this.adapterId = adapterId;
    this.partition = partition;
    sortKey = key.getSortKey();
    dataId = key.getDataId();
    duplicates = key.getNumDuplicates();
    attributeValues = new GeoWaveValue[] {value};
  }

  @Override
  public byte[] getDataId() {
    return dataId;
  }

  @Override
  public short getAdapterId() {
    return adapterId;
  }

  @Override
  public byte[] getSortKey() {
    return sortKey;
  }

  @Override
  public byte[] getPartitionKey() {
    return partition;
  }

  @Override
  public int getNumberOfDuplicates() {
    return duplicates;
  }

  public String[] getFiles() {
    // this is intentionally not threadsafe because it isn't required
    if (mergedFileNames == null) {
      return new String[] {fileName};
    } else {
      return ArrayUtils.add(mergedFileNames.toArray(new String[0]), fileName);
    }
  }

  @Override
  public void mergeRow(final MergeableGeoWaveRow row) {
    super.mergeRow(row);
    if (row instanceof FileSystemRow) {
      // this is intentionally not threadsafe because it isn't required
      if (mergedFileNames == null) {
        mergedFileNames = new ArrayList<>();
      }
      Arrays.stream(((FileSystemRow) row).getFiles()).forEach(r -> mergedFileNames.add(r));
    }
  }
}
