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
package org.locationtech.geowave.core.store.data;

import java.util.List;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.flatten.FlattenedFieldInfo;
import org.locationtech.geowave.core.store.flatten.FlattenedUnreadData;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

/**
 * Consults adapter to lookup field readers based on bitmasked fieldIds when
 * converting unknown data to adapter extended values
 *
 * @since 0.9.1
 */
public class DeferredReadCommonIndexedPersistenceEncoding extends
		AbstractAdapterPersistenceEncoding
{

	private final FlattenedUnreadData unreadData;

	public DeferredReadCommonIndexedPersistenceEncoding(
			final short adapterId,
			final ByteArray dataId,
			final ByteArray partitionKey,
			final ByteArray sortKey,
			final int duplicateCount,
			final PersistentDataset<CommonIndexValue> commonData,
			final FlattenedUnreadData unreadData ) {
		super(
				adapterId,
				dataId,
				partitionKey,
				sortKey,
				duplicateCount,
				commonData,
				new PersistentDataset<byte[]>(),
				new PersistentDataset<>());
		this.unreadData = unreadData;

	}

	@Override
	public void convertUnknownValues(
			final DataTypeAdapter<?> adapter,
			final CommonIndexModel model ) {
		if (unreadData != null) {
			final List<FlattenedFieldInfo> fields = unreadData.finishRead();
			for (final FlattenedFieldInfo field : fields) {
				String fieldName = adapter.getFieldNameForPosition(
						model,
						field.getFieldPosition());
				if (fieldName == null) {
					fieldName = adapter.getFieldNameForPosition(
							model,
							field.getFieldPosition());
				}
				final FieldReader<Object> reader = adapter.getReader(fieldName);
				final Object value = reader.readField(field.getValue());
				adapterExtendedData.addValue(
						fieldName,
						value);
			}
		}
	}

}
