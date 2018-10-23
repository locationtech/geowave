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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class StatisticsRowIterator implements
		CloseableIterator<GeoWaveMetadata>
{
	final private CloseableIterator<GeoWaveMetadata> it;
	private InternalDataStatistics<?, ?, ?> nextVal = null;

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
		InternalDataStatistics<?, ?, ?> currentStatistics = nextVal;

		nextVal = null;
		while (it.hasNext()) {
			final GeoWaveMetadata row = it.next();

			final InternalDataStatistics<?, ?, ?> statEntry = entryToValue(row);

			if (currentStatistics == null) {
				currentStatistics = statEntry;
			}
			else {
				if (statEntry.getType().equals(
						currentStatistics.getType()) && statEntry.getAdapterId().equals(
						currentStatistics.getAdapterId()) && statEntry.getExtendedId().equals(
						currentStatistics.getExtendedId())) {
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

	protected InternalDataStatistics<?, ?, ?> entryToValue(
			final GeoWaveMetadata entry ) {
		final InternalDataStatistics<?, ?, ?> basicStats = (InternalDataStatistics<?, ?, ?>) PersistenceUtils
				.fromBinary(entry.getValue());
		if (basicStats != null) {
			DataStatisticsStoreImpl.setFields(
					entry,
					basicStats,
					ByteArrayUtils.byteArrayToShort(entry.getSecondaryId()));
		}
		return basicStats;
	}

	protected GeoWaveMetadata statsToMetadata(
			final InternalDataStatistics<?, ?, ?> stats ) {
		return new GeoWaveMetadata(
				DataStatisticsStoreImpl.getPrimaryId(
						stats.getType(),
						stats.getExtendedId()).getBytes(),
				ByteArrayUtils.shortToByteArray(stats.getAdapterId()),
				stats.getVisibility(),
				PersistenceUtils.toBinary(stats));
	}

	@Override
	public void close() {
		it.close();
	}
}
