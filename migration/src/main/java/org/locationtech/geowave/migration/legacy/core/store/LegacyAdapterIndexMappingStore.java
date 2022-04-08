/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.migration.legacy.core.store;

import java.util.Arrays;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Streams;

public class LegacyAdapterIndexMappingStore extends
    AbstractGeoWavePersistence<LegacyAdapterToIndexMapping> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LegacyAdapterIndexMappingStore.class);

  public LegacyAdapterIndexMappingStore(
      final DataStoreOperations operations,
      final DataStoreOptions options) {
    super(operations, options, MetadataType.LEGACY_INDEX_MAPPINGS);
  }

  public boolean mappingExists(final AdapterToIndexMapping persistedObject) {
    return objectExists(
        new ByteArray(ByteArrayUtils.shortToByteArray(persistedObject.getAdapterId())),
        null);
  }

  @Override
  protected ByteArray getPrimaryId(final LegacyAdapterToIndexMapping persistedObject) {
    return new ByteArray(ByteArrayUtils.shortToByteArray(persistedObject.getAdapterId()));
  }

  public LegacyAdapterToIndexMapping getIndicesForAdapter(final short adapterId) {

    final LegacyAdapterToIndexMapping mapping =
        super.internalGetObject(
            new ByteArray(ByteArrayUtils.shortToByteArray(adapterId)),
            null,
            false);
    return (mapping != null) ? mapping : new LegacyAdapterToIndexMapping(adapterId, new String[0]);
  }

  public void addAdapterIndexMapping(final LegacyAdapterToIndexMapping mapping) {
    final ByteArray adapterId =
        new ByteArray(ByteArrayUtils.shortToByteArray(mapping.getAdapterId()));
    if (objectExists(adapterId, null)) {
      final LegacyAdapterToIndexMapping oldMapping = super.getObject(adapterId, null);
      if (!oldMapping.equals(mapping)) {
        // combine the 2 arrays and remove duplicates (get unique set of
        // index names)
        final String[] uniqueCombinedIndices =
            Streams.concat(
                Arrays.stream(mapping.getIndexNames()),
                Arrays.stream(oldMapping.getIndexNames())).distinct().toArray(
                    size -> new String[size]);
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Updating indices for datatype to " + ArrayUtils.toString(uniqueCombinedIndices));
        }
        remove(adapterId);
        addObject(new LegacyAdapterToIndexMapping(mapping.getAdapterId(), uniqueCombinedIndices));
      }
    } else {
      addObject(mapping);
    }
  }

  public void remove(final short internalAdapterId) {
    super.remove(new ByteArray(ByteArrayUtils.shortToByteArray(internalAdapterId)));
  }

  public boolean remove(final short internalAdapterId, final String indexName) {
    final ByteArray adapterId = new ByteArray(ByteArrayUtils.shortToByteArray(internalAdapterId));
    if (!objectExists(adapterId, null)) {
      return false;
    }

    final LegacyAdapterToIndexMapping oldMapping = super.getObject(adapterId, null);

    boolean found = false;
    final String[] indexNames = oldMapping.getIndexNames();
    for (int i = 0; i < indexNames.length; i++) {
      if (indexNames[i].compareTo(indexName) == 0) {
        found = true;
        break;
      }
    }

    if (!found) {
      return false;
    }

    if (indexNames.length > 1) {
      // update the mapping and reset it
      final String[] newIndices = new String[indexNames.length - 1];
      int count = 0;
      for (int i = 0; i < indexNames.length; i++) {
        if (indexNames[i].compareTo(indexName) == 0) {
          continue;
        } else {
          newIndices[count] = indexNames[i];
          count++;
        }
      }
      remove(adapterId);
      addObject(new LegacyAdapterToIndexMapping(internalAdapterId, newIndices));
    } else {
      // otherwise just remove the mapping
      remove(adapterId);
    }

    return true;
  }
}
