/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.metadata;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Streams;

/**
 * This class will persist Adapter Index Mappings within an Accumulo table for
 * GeoWave metadata. The mappings will be persisted in an "AIM" column family.
 *
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 *
 * Objects are maintained with regard to visibility. The assumption is that a
 * mapping between an adapter and indexing is consistent across all visibility
 * constraints.
 */
public class AdapterIndexMappingStoreImpl extends
		AbstractGeoWavePersistence<AdapterToIndexMapping> implements
		AdapterIndexMappingStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AdapterIndexMappingStoreImpl.class);

	public AdapterIndexMappingStoreImpl(
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		super(
				operations,
				options,
				MetadataType.AIM);
	}

	public boolean mappingExists(
			final AdapterToIndexMapping persistedObject ) {
		return objectExists(
				new ByteArray(
						ByteArrayUtils.shortToByteArray(persistedObject.getAdapterId())),
				null);
	}

	@Override
	protected ByteArray getPrimaryId(
			final AdapterToIndexMapping persistedObject ) {
		return new ByteArray(
				ByteArrayUtils.shortToByteArray(persistedObject.getAdapterId()));
	}

	@Override
	public AdapterToIndexMapping getIndicesForAdapter(
			final short adapterId ) {

		final AdapterToIndexMapping mapping = super.internalGetObject(
				new ByteArray(
						ByteArrayUtils.shortToByteArray(adapterId)),
				null,
				false,
				null);
		return (mapping != null) ? mapping : new AdapterToIndexMapping(
				adapterId,
				new String[0]);
	}

	@Override
	public void addAdapterIndexMapping(
			final AdapterToIndexMapping mapping ) {
		final ByteArray adapterId = new ByteArray(
				ByteArrayUtils
						.shortToByteArray(
								mapping.getAdapterId()));
		if (objectExists(
				adapterId,
				null)) {
			final AdapterToIndexMapping oldMapping = super.getObject(
					adapterId,
					null,
					null);
			if (!oldMapping
					.equals(
							mapping)) {
				// combine the 2 arrays and remove duplicates (get unique set of
				// index names)
				final String[] uniqueCombinedIndices = Streams
						.concat(
								Arrays
										.stream(
												mapping.getIndexNames()),
								Arrays
										.stream(
												oldMapping.getIndexNames()))
						.distinct()
						.toArray(
								size -> new String[size]);
				if (LOGGER.isInfoEnabled()) {
					LOGGER
							.info(
									"Updating indices for datatype to " + ArrayUtils
											.toString(
													uniqueCombinedIndices));
				}
				remove(
						adapterId);
				addObject(
						new AdapterToIndexMapping(
								mapping.getAdapterId(),
								uniqueCombinedIndices));
			}
		}
		else {
			addObject(
					mapping);
		}

	}

	@Override
	public void remove(
			final short internalAdapterId ) {
		super.remove(new ByteArray(
				ByteArrayUtils.shortToByteArray(internalAdapterId)));
	}
}
