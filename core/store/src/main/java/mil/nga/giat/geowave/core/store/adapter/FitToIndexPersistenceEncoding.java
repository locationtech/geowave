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
package mil.nga.giat.geowave.core.store.adapter;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class FitToIndexPersistenceEncoding extends
		AdapterPersistenceEncoding
{
	private final List<ByteArrayId> insertionIds = new ArrayList<ByteArrayId>();

	public FitToIndexPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<Object> adapterExtendedData,
			final ByteArrayId insertionId ) {
		super(
				adapterId,
				dataId,
				commonData,
				adapterExtendedData);
		insertionIds.add(insertionId);
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final PrimaryIndex index ) {
		return insertionIds;
	}

	@Override
	public boolean isDeduplicationEnabled() {
		return false;
	}

}
