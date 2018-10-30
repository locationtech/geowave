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
package org.locationtech.geowave.core.store.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.query.filter.FilterList;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseSecondaryIndexDataStore implements
		SecondaryIndexDataStore,
		Closeable
{

	private final static Logger LOGGER = LoggerFactory.getLogger(BaseSecondaryIndexDataStore.class);
	protected final Map<String, RowWriter> writerCache = new HashMap<>();
	protected final static byte[] EMPTY_VALUE = new byte[0];

	public BaseSecondaryIndexDataStore() {}

	@Override
	public void storeJoinEntry(
			final String secondaryIndexName,
			final ByteArray indexedAttributeValue,
			final String typeName,
			final String indexedAttributeFieldName,
			final String primaryIndexName,
			final ByteArray primaryPartitionKey,
			final ByteArray primarySortKey,
			final ByteArray attributeVisibility ) {
		try {
			final RowWriter writer = getWriter(secondaryIndexName);
			if (writer != null) {
				writer.write(buildJoinMutation(
						indexedAttributeValue.getBytes(),
						StringUtils.stringToBinary(typeName),
						StringUtils.stringToBinary(indexedAttributeFieldName),
						StringUtils.stringToBinary(primaryIndexName),
						primaryPartitionKey.getBytes(),
						primarySortKey.getBytes(),
						attributeVisibility.getBytes()));
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to build secondary index row mutation.",
					e);
		}
	}

	@Override
	public void storeEntry(
			final String secondaryIndexName,
			final ByteArray indexedAttributeValue,
			final String typeName,
			final String indexedAttributeFieldName,
			final ByteArray dataId,
			final GeoWaveValue... values ) {
		try {
			final RowWriter writer = getWriter(secondaryIndexName);
			if (writer != null) {
				for (final GeoWaveValue v : values) {
					writer.write(buildMutation(
							indexedAttributeValue.getBytes(),
							StringUtils.stringToBinary(typeName),
							StringUtils.stringToBinary(indexedAttributeFieldName),
							dataId.getBytes(),
							v.getFieldMask(),
							v.getValue(),
							v.getVisibility()));
				}
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to build secondary index row mutation.",
					e);
		}
	}

	protected static QueryFilter getFilter(
			final List<QueryFilter> constraints ) {
		final QueryFilter filter;
		if (constraints.isEmpty()) {
			filter = null;
		}
		else if (constraints.size() == 1) {
			filter = constraints.get(0);
		}
		else {
			filter = new FilterList(
					false,
					constraints);
		}
		return filter;
	}

	@Override
	public void deleteJoinEntry(
			final String secondaryIndexName,
			final ByteArray indexedAttributeValue,
			final String typeName,
			final String indexedAttributeFieldName,
			final String primaryIndexName,
			final ByteArray primaryIndexPartitionKey,
			final ByteArray primaryIndexSortKey,
			final ByteArray attributeVisibility ) {
		try {
			final RowWriter writer = getWriter(secondaryIndexName);
			if (writer != null) {
				writer.write(buildJoinDeleteMutation(
						indexedAttributeValue.getBytes(),
						StringUtils.stringToBinary(typeName),
						StringUtils.stringToBinary(indexedAttributeFieldName),
						StringUtils.stringToBinary(primaryIndexName),
						primaryIndexPartitionKey.getBytes(),
						primaryIndexSortKey.getBytes()));
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Failed to delete from secondary index.",
					e);
		}
	}

	@Override
	public void deleteEntry(
			final String secondaryIndexName,
			final ByteArray indexedAttributeValue,
			final String typeName,
			final String indexedAttributeFieldName,
			final ByteArray dataId,
			final GeoWaveValue... values ) {
		try {
			final RowWriter writer = getWriter(secondaryIndexName);
			if (writer != null) {
				for (final GeoWaveValue v : values) {
					writer.write(buildFullDeleteMutation(
							indexedAttributeValue.getBytes(),
							StringUtils.stringToBinary(typeName),
							StringUtils.stringToBinary(indexedAttributeFieldName),
							dataId.getBytes(),
							v.getFieldMask()));
				}
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Failed to delete from secondary index.",
					e);
		}
	}

	@Override
	public void removeAll() {
		close();
		writerCache.clear();
	}

	@Override
	public void close() {
		for (final RowWriter writer : writerCache.values()) {
			try {
				writer.close();
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to close secondary index writer",
						e);
			}
		}
		writerCache.clear();
	}

	@Override
	public void flush() {
		close();
	}

	protected abstract GeoWaveRow buildJoinMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexPartitionKey,
			final byte[] primaryIndexSortKey,
			final byte[] attributeVisibility )
			throws IOException;

	protected abstract GeoWaveRow buildMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] dataId,
			final byte[] fieldId,
			final byte[] fieldValue,
			final byte[] fieldVisibility )
			throws IOException;

	protected abstract GeoWaveRow buildJoinDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexPartitionKey,
			final byte[] primaryIndexSortKey )
			throws IOException;

	protected abstract GeoWaveRow buildFullDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] dataId,
			final byte[] fieldId )
			throws IOException;

	protected abstract RowWriter getWriter(
			String secondaryIndexName );

}
