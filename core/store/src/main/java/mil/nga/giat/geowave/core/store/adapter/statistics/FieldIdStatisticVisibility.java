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
package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;

public class FieldIdStatisticVisibility<T> implements
		EntryVisibilityHandler<T>
{
	private final ByteArrayId fieldId;

	public FieldIdStatisticVisibility(
			final ByteArrayId fieldId ) {
		this.fieldId = fieldId;
	}

	@Override
	public byte[] getVisibility(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final FieldInfo<?> f : entryInfo.getFieldInfo()) {
			if (f.getDataValue().getId().equals(
					fieldId)) {
				return f.getVisibility();
			}
		}
		return null;
	}
}
