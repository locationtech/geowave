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
package org.locationtech.geowave.datastore.dynamodb.util;

import java.util.Iterator;
import java.util.Map;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatistics;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class DynamoDBStatisticsIterator implements
		CloseableIterator<GeoWaveMetadata>
{
	final private Iterator<Map<String, AttributeValue>> it;
	private DataStatistics<?> nextVal = null;

	public DynamoDBStatisticsIterator(
			final Iterator<Map<String, AttributeValue>> resultIterator ) {
		it = resultIterator;
	}

	@Override
	public boolean hasNext() {
		return (nextVal != null) || it.hasNext();
	}

	@Override
	public GeoWaveMetadata next() {
		DataStatistics<?> currentStatistics = nextVal;

		nextVal = null;
		while (it.hasNext()) {
			final Map<String, AttributeValue> row = it.next();

			final DataStatistics<?> statEntry = entryToValue(row);

			if (currentStatistics == null) {
				currentStatistics = statEntry;
			}
			else {
				if (statEntry.getStatisticsId().equals(
						currentStatistics.getStatisticsId()) && statEntry.getInternalDataAdapterId().equals(
						currentStatistics.getInternalDataAdapterId())) {
					currentStatistics.merge(statEntry);
				}
				else {
					nextVal = statEntry;
					break;
				}
			}
		}

		return statsToMetadata(currentStatistics);
	}

	@Override
	public void close() {
		// Close is a no-op for dynamodb client
	}

	protected DataStatistics<?> entryToValue(
			final Map<String, AttributeValue> entry ) {
		final DataStatistics<?> stats = (DataStatistics<?>) PersistenceUtils.fromBinary(DynamoDBUtils.getValue(entry));

		if (stats != null) {
			stats.setInternalDataAdapterId(ByteArrayUtils.byteArrayToShort(DynamoDBUtils.getSecondaryId(entry)));
			stats.setStatisticsId(new ByteArrayId(
					DynamoDBUtils.getPrimaryId(entry)));
		}

		return stats;
	}

	protected GeoWaveMetadata statsToMetadata(
			final DataStatistics<?> stats ) {
		return new GeoWaveMetadata(
				stats.getStatisticsId().getBytes(),
				ByteArrayUtils.shortToByteArray(stats.getInternalDataAdapterId()),
				null,
				PersistenceUtils.toBinary(stats));
	}
}
