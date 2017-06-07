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
package mil.nga.giat.geowave.datastore.accumulo.encoding;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * Consults adapter to lookup field readers based on bitmasked fieldIds when
 * converting unknown data to adapter extended values
 *
 * @since 0.9.1
 */
public class AccumuloCommonIndexedPersistenceEncoding extends
		AbstractAdapterPersistenceEncoding
{

	private final FlattenedUnreadData unreadData;

	public AccumuloCommonIndexedPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final ByteArrayId indexInsertionId,
			final int duplicateCount,
			final PersistentDataset<CommonIndexValue> commonData,
			final FlattenedUnreadData unreadData ) {
		super(
				adapterId,
				dataId,
				indexInsertionId,
				duplicateCount,
				commonData,
				new PersistentDataset<byte[]>(),
				new PersistentDataset<Object>());
		this.unreadData = unreadData;

	}

	@Override
	public void convertUnknownValues(
			final DataAdapter<?> adapter,
			final CommonIndexModel model ) {
		if (unreadData != null) {
			final List<FlattenedFieldInfo> fields = unreadData.finishRead();
			for (final FlattenedFieldInfo field : fields) {
				final ByteArrayId fieldId = adapter.getFieldIdForPosition(
						model,
						field.getFieldPosition());
				final FieldReader<Object> reader = adapter.getReader(fieldId);
				final Object value = reader.readField(field.getValue());
				adapterExtendedData.addValue(new PersistentValue<Object>(
						fieldId,
						value));
			}
		}
	}

}
