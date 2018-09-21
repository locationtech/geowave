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
package org.locationtech.geowave.core.store.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatistics;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class StatisticsRowIterator implements
		CloseableIterator<GeoWaveMetadata>
{
	final private CloseableIterator<GeoWaveMetadata> it;
	private DataStatistics<?> nextVal = null;

	public StatisticsRowIterator(
			final CloseableIterator<GeoWaveMetadata> resultIterator,
			final String... authorizations ) {
		if ((authorizations != null) && (authorizations.length > 0)) {
			final Set<String> authorizationsSet = new HashSet<>(
					Arrays.asList(authorizations));
			it = new CloseableIteratorWrapper<>(
					resultIterator,
					Iterators.filter(
							resultIterator,
							new Predicate<GeoWaveMetadata>() {
								@Override
								public boolean apply(
										final GeoWaveMetadata input ) {
									String visibility = "";
									if (input.getVisibility() != null) {
										visibility = StringUtils.stringFromBinary(input.getVisibility());
									}
									return VisibilityExpression.evaluate(
											visibility,
											authorizationsSet);
								}
							}));
		}
		else {
			it = new CloseableIteratorWrapper<>(
					resultIterator,
					Iterators.filter(
							resultIterator,
							new Predicate<GeoWaveMetadata>() {
								@Override
								public boolean apply(
										final GeoWaveMetadata input ) {
									// we don't have any authorizations
									// so this row cannot have any
									// visibilities
									return (input.getVisibility() == null) || (input.getVisibility().length == 0);
								}
							}));
		}
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

	protected DataStatistics<?> entryToValue(
			final GeoWaveMetadata entry ) {
		final DataStatistics<?> stats = (DataStatistics<?>) PersistenceUtils.fromBinary(entry.getValue());

		if (stats != null) {
			stats.setInternalDataAdapterId(ByteArrayUtils.byteArrayToShort(entry.getSecondaryId()));
			stats.setStatisticsId(new ByteArrayId(
					entry.getPrimaryId()));
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

	@Override
	public void close()
			throws IOException {
		it.close();
	}
}
