/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.datastore.hbase.metadata;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

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
public class HBaseAdapterIndexMappingStore extends
		AbstractHBasePersistence<AdapterToIndexMapping> implements
		AdapterIndexMappingStore
{
	protected static final String ADAPTER_INDEX_CF = "AIM";

	public HBaseAdapterIndexMappingStore(
			final BasicHBaseOperations hbaseOperations ) {
		super(
				hbaseOperations);
	}

	public boolean mappingExists(
			final AdapterToIndexMapping persistedObject ) {
		return objectExists(
				persistedObject.getAdapterId(),
				null);
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final AdapterToIndexMapping persistedObject ) {
		return persistedObject.getAdapterId();
	}

	@Override
	protected String getPersistenceTypeName() {
		return ADAPTER_INDEX_CF;
	}

	@Override
	public AdapterToIndexMapping getIndicesForAdapter(
			final ByteArrayId adapterId ) {
		final AdapterToIndexMapping mapping = super.getObject(
				adapterId,
				null,
				null);
		return (mapping != null) ? mapping : new AdapterToIndexMapping(
				adapterId,
				new ByteArrayId[0]);
	}

	@Override
	public void addAdapterIndexMapping(
			final AdapterToIndexMapping mapping )
			throws MismatchedIndexToAdapterMapping {
		if (objectExists(
				mapping.getAdapterId(),
				null)) {
			final AdapterToIndexMapping oldMapping = super.getObject(
					mapping.getAdapterId(),
					null,
					null);
			if (!oldMapping.equals(mapping)) {
				throw new MismatchedIndexToAdapterMapping(
						oldMapping); // HPFortify FP: accumulo example stores
										// hardcoded password
			}
		}
		else {
			addObject(mapping);
		}

	}

	@Override
	public void remove(
			final ByteArrayId adapterId ) {
		super.deleteObject(
				adapterId,
				null);

	}
}
