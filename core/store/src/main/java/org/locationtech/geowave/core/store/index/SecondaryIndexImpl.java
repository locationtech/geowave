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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.data.PersistentValue;

/**
 * This class fully describes everything necessary to index data within GeoWave
 * using secondary indexing. <br>
 * The key components are the indexing strategy and the common index model. <br>
 * <br>
 * Attributes for SecondaryIndex include:<br>
 * indexStrategy = array of fieldIndexStrategy (numeric, temporal or text)<br>
 * fieldId<br>
 * associatedStatistics <br>
 * secondaryIndexType - (join, full, partial)<br>
 * secondaryIndexId - <br>
 * partialFieldIds - list of fields that are part of the ...<br>
 */

public class SecondaryIndexImpl<T> implements
		SecondaryIndex<FilterableConstraints, List<PersistentValue<?>>>
{
	private static final String TABLE_PREFIX = "GEOWAVE_2ND_IDX_";
	private FieldIndexStrategy<?, ?> indexStrategy;
	private String fieldName;
	private List<InternalDataStatistics<T, ?, ?>> associatedStatistics;
	private SecondaryIndexType secondaryIndexType;
	private String secondaryIndexName;
	private List<String> partialFieldNames;

	public SecondaryIndexImpl() {}

	public SecondaryIndexImpl(
			final FieldIndexStrategy<?, ?> indexStrategy,
			final String fieldName,
			final List<InternalDataStatistics<T, ?, ?>> associatedStatistics,
			final SecondaryIndexType secondaryIndexType ) {
		this(
				indexStrategy,
				fieldName,
				associatedStatistics,
				secondaryIndexType,
				Collections.<String> emptyList());
	}

	public SecondaryIndexImpl(
			final FieldIndexStrategy<?, ?> indexStrategy,
			final String fieldName,
			final List<InternalDataStatistics<T, ?, ?>> associatedStatistics,
			final SecondaryIndexType secondaryIndexType,
			final List<String> partialFieldNames ) {
		super();
		this.indexStrategy = indexStrategy;
		this.fieldName = fieldName;
		this.associatedStatistics = associatedStatistics;
		this.secondaryIndexType = secondaryIndexType;
		this.secondaryIndexName = TABLE_PREFIX + indexStrategy.getId() + "_" + secondaryIndexType.getValue();
		this.partialFieldNames = partialFieldNames;
	}

	@SuppressWarnings({
		"unchecked",
		"rawtypes"
	})
	@Override
	public FieldIndexStrategy getIndexStrategy() {
		return indexStrategy;
	}

	public String getFieldName() {
		return fieldName;
	}

	@Override
	public String getName() {
		return secondaryIndexName;
	}

	public List<InternalDataStatistics<T, ?, ?>> getAssociatedStatistics() {
		return associatedStatistics;
	}

	public SecondaryIndexType getSecondaryIndexType() {
		return secondaryIndexType;
	}

	public List<String> getPartialFieldNames() {
		return partialFieldNames;
	}

	@Override
	public int hashCode() {
		return getName().hashCode();
	}

	/**
	 * Compare this object to the one passed as parameter to see if same object,
	 * same class and that id is the same.
	 */

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final SecondaryIndexImpl<?> other = (SecondaryIndexImpl<?>) obj;
		return getName().equals(
				other.getName());
	}

	@Override
	public byte[] toBinary() {
		final byte[] indexStrategyBinary = PersistenceUtils.toBinary(indexStrategy);
		final byte[] fieldIdBinary = StringUtils.stringToBinary(fieldName);
		final byte[] secondaryIndexTypeBinary = StringUtils.stringToBinary(secondaryIndexType.getValue());
		final List<Persistable> persistables = new ArrayList<>();
		for (final InternalDataStatistics<T, ?, ?> dataStatistics : associatedStatistics) {
			persistables.add(dataStatistics);
		}
		final byte[] persistablesBinary = PersistenceUtils.toBinary(persistables);
		final boolean handlePartials = ((partialFieldNames != null) && !partialFieldNames.isEmpty());
		int partialsLength = 0;
		byte[] partialsBinary = null;
		if (handlePartials) {
			int totalLength = 0;
			for (final String partialFieldName : partialFieldNames) {
				totalLength += StringUtils.stringToBinary(partialFieldName).length;
			}
			final ByteBuffer allPartials = ByteBuffer.allocate(totalLength + (partialFieldNames.size() * 4));
			for (final String partialFieldName : partialFieldNames) {
				final byte[] partialFieldBytes = StringUtils.stringToBinary(partialFieldName);
				allPartials.putInt(partialFieldBytes.length);
				allPartials.put(partialFieldBytes);
			}
			partialsLength = allPartials.array().length;
			partialsBinary = allPartials.array();
		}
		final ByteBuffer buf = ByteBuffer.allocate(indexStrategyBinary.length + fieldIdBinary.length
				+ secondaryIndexTypeBinary.length + 20 + persistablesBinary.length + partialsLength
				+ (partialsLength > 0 ? 4 : 0));
		buf.putInt(indexStrategyBinary.length);
		buf.putInt(fieldIdBinary.length);
		buf.putInt(secondaryIndexTypeBinary.length);
		buf.putInt(persistablesBinary.length);
		buf.putInt(handlePartials ? partialFieldNames.size() : 0);
		buf.put(indexStrategyBinary);
		buf.put(fieldIdBinary);
		buf.put(secondaryIndexTypeBinary);
		buf.put(persistablesBinary);
		if (handlePartials) {
			buf.putInt(partialsLength);
			buf.put(partialsBinary);
		}
		return buf.array();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int indexStrategyLength = buf.getInt();
		final int fieldNameLength = buf.getInt();
		final int secondaryIndexTypeLength = buf.getInt();
		final int persistablesBinaryLength = buf.getInt();
		final int numPartials = buf.getInt();
		final byte[] indexStrategyBinary = new byte[indexStrategyLength];
		final byte[] fieldNameBinary = new byte[fieldNameLength];
		final byte[] secondaryIndexTypeBinary = new byte[secondaryIndexTypeLength];
		buf.get(indexStrategyBinary);
		buf.get(fieldNameBinary);
		buf.get(secondaryIndexTypeBinary);

		indexStrategy = (FieldIndexStrategy<?, ?>) PersistenceUtils.fromBinary(indexStrategyBinary);

		fieldName = StringUtils.stringFromBinary(fieldNameBinary);

		secondaryIndexType = SecondaryIndexType.valueOf(StringUtils.stringFromBinary(secondaryIndexTypeBinary));

		final byte[] persistablesBinary = new byte[persistablesBinaryLength];
		buf.get(persistablesBinary);
		final List<Persistable> persistables = PersistenceUtils.fromBinaryAsList(persistablesBinary);
		for (final Persistable persistable : persistables) {
			associatedStatistics.add((InternalDataStatistics<T, ?, ?>) persistable);
		}
		secondaryIndexName = indexStrategy.getId() + "_" + secondaryIndexType.getValue();

		if (numPartials > 0) {
			partialFieldNames = new ArrayList<>();
			final int partialsLength = buf.getInt();
			final byte[] partialsBinary = new byte[partialsLength];
			final ByteBuffer partialsBB = ByteBuffer.wrap(partialsBinary);
			for (int i = 0; i < numPartials; i++) {
				final int currPartialLength = partialsBB.getInt();
				final byte[] currPartialBinary = new byte[currPartialLength];
				partialFieldNames.add(StringUtils.stringFromBinary(currPartialBinary));
			}
		}
	}
}
