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
package org.locationtech.geowave.datastore.cassandra.operations;

import java.io.IOException;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatistics;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

public class CassandraStatisticsIterator implements
		CloseableIterator<GeoWaveMetadata>
{
	final private CloseableIterator<GeoWaveMetadata> it;
	private DataStatistics<?> nextVal = null;

	public CassandraStatisticsIterator(
			final CloseableIterator<GeoWaveMetadata> resultIterator ) {
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
			final GeoWaveMetadata row = it.next();

			final DataStatistics<?> statEntry = entryToValue(row);

			if (currentStatistics == null) {
				currentStatistics = statEntry;
			}
			else {
				if (statEntry.getStatisticsType().equals(
						currentStatistics.getStatisticsType()) && statEntry.getInternalDataAdapterId().equals(
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

	protected DataStatistics<?> entryToValue(
			final GeoWaveMetadata entry ) {
		final DataStatistics<?> stats = (DataStatistics<?>) PersistenceUtils.fromBinary(entry.getValue());

		if (stats != null) {
			stats.setInternalDataAdapterId(ByteArrayUtils.byteArrayToShort(entry.getSecondaryId()));
			stats.setStatisticsType(new ByteArrayId(
					entry.getPrimaryId()));
		}

		return stats;
	}

	protected GeoWaveMetadata statsToMetadata(
			final DataStatistics<?> stats ) {
		return new GeoWaveMetadata(
				stats.getStatisticsType().getBytes(),
				ByteArrayUtils.shortToByteArray(stats.getInternalDataAdapterId()),
				null,
				PersistenceUtils.toBinary(stats));
	}

	@Override
	public void close()
			throws IOException {
		it.close();
	}
}
