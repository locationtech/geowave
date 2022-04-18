/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.metadata;

import java.util.Set;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Lists;

/**
 * This class will persist Adapter Index Mappings within a table for GeoWave metadata. The mappings
 * will be persisted in an "INDEX_MAPPINGS" metadata table.
 *
 * <p> There is an LRU cache associated with it so staying in sync with external updates is not
 * practical - it assumes the objects are not updated often or at all. The objects are stored in
 * their own table.
 *
 * <p> Objects are maintained with regard to visibility. The assumption is that a mapping between an
 * adapter and indexing is consistent across all visibility constraints.
 */
public class AdapterIndexMappingStoreImpl extends AbstractGeoWavePersistence<AdapterToIndexMapping>
    implements
    AdapterIndexMappingStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdapterIndexMappingStoreImpl.class);

  public AdapterIndexMappingStoreImpl(
      final DataStoreOperations operations,
      final DataStoreOptions options) {
    super(operations, options, MetadataType.INDEX_MAPPINGS);
  }

  public boolean mappingExists(final AdapterToIndexMapping persistedObject) {
    return objectExists(getPrimaryId(persistedObject), getSecondaryId(persistedObject));
  }

  @Override
  protected ByteArray getPrimaryId(final AdapterToIndexMapping persistedObject) {
    return new ByteArray(ByteArrayUtils.shortToByteArray(persistedObject.getAdapterId()));
  }

  @Override
  protected ByteArray getSecondaryId(final AdapterToIndexMapping persistedObject) {
    return new ByteArray(StringUtils.stringToBinary(persistedObject.getIndexName()));
  }

  @Override
  public AdapterToIndexMapping[] getIndicesForAdapter(final short adapterId) {
    final Set<Object> indexMappings = Sets.newHashSet();
    try (CloseableIterator<AdapterToIndexMapping> iter =
        super.internalGetObjects(
            new MetadataQuery(ByteArrayUtils.shortToByteArray(adapterId), null, false))) {
      while (iter.hasNext()) {
        indexMappings.add(iter.next());
      }
    }
    return indexMappings.toArray(new AdapterToIndexMapping[indexMappings.size()]);
  }

  @Override
  public AdapterToIndexMapping getMapping(final short adapterId, final String indexName) {
    if (indexName.equals(DataIndexUtils.DATA_ID_INDEX.getName())) {
      return new AdapterToIndexMapping(adapterId, indexName, Lists.newArrayList());
    }
    final ByteArray primaryId = new ByteArray(ByteArrayUtils.shortToByteArray(adapterId));
    final ByteArray secondaryId = new ByteArray(StringUtils.stringToBinary(indexName));
    return super.getObject(primaryId, secondaryId);
  }

  @Override
  public void addAdapterIndexMapping(final AdapterToIndexMapping mapping) {
    final ByteArray primaryId = getPrimaryId(mapping);
    final ByteArray secondaryId = getSecondaryId(mapping);
    if (objectExists(primaryId, secondaryId)) {
      LOGGER.info("Adapter to index mapping already existed, skipping add.");
    } else {
      addObject(mapping);
    }
  }

  @Override
  public void remove(final short internalAdapterId) {
    super.remove(new ByteArray(ByteArrayUtils.shortToByteArray(internalAdapterId)));
  }

  @Override
  public boolean remove(final short internalAdapterId, final String indexName) {
    final ByteArray adapterId = new ByteArray(ByteArrayUtils.shortToByteArray(internalAdapterId));
    final ByteArray secondaryId = new ByteArray(StringUtils.stringToBinary(indexName));
    if (!objectExists(adapterId, secondaryId)) {
      return false;
    }

    return super.deleteObject(adapterId, secondaryId);
  }
}
