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
package org.locationtech.geowave.adapter.raster.stats;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.locationtech.geowave.adapter.raster.FitToIndexGridCoverage;
import org.locationtech.geowave.adapter.raster.Resolution;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.BaseStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.BaseStatisticsType;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.opengis.coverage.grid.GridCoverage;

public class OverviewStatistics extends
		AbstractDataStatistics<GridCoverage, Resolution[], BaseStatisticsQueryBuilder<Resolution[]>>
{
	public static final BaseStatisticsType<Resolution[]> STATS_TYPE = new BaseStatisticsType<>(
			"OVERVIEW");

	private Resolution[] resolutions = new Resolution[] {};

	public OverviewStatistics() {
		this(
				null);
	}

	public OverviewStatistics(
			final Short adapterId ) {
		super(
				adapterId,
				STATS_TYPE);
	}

	@Override
	public byte[] toBinary() {
		synchronized (this) {
			final List<byte[]> resolutionBinaries = new ArrayList<>(
					resolutions.length);
			int byteCount = 4; // an int for the list size
			for (final Resolution res : resolutions) {
				final byte[] resBinary = PersistenceUtils.toBinary(res);
				resolutionBinaries.add(resBinary);
				byteCount += (resBinary.length + 4); // an int for the binary
														// size
			}

			final ByteBuffer buf = super.binaryBuffer(byteCount);
			buf.putInt(resolutionBinaries.size());
			for (final byte[] resBinary : resolutionBinaries) {
				buf.putInt(resBinary.length);
				buf.put(resBinary);
			}
			return buf.array();
		}
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = super.binaryBuffer(bytes);
		final int resLength = buf.getInt();
		synchronized (this) {
			resolutions = new Resolution[resLength];
			for (int i = 0; i < resolutions.length; i++) {
				final byte[] resBytes = new byte[buf.getInt()];
				buf.get(resBytes);
				resolutions[i] = (Resolution) PersistenceUtils.fromBinary(resBytes);
			}
		}
	}

	@Override
	public void entryIngested(
			final GridCoverage entry,
			final GeoWaveRow... geoWaveRows ) {
		if (entry instanceof FitToIndexGridCoverage) {
			final FitToIndexGridCoverage fitEntry = (FitToIndexGridCoverage) entry;
			synchronized (this) {
				resolutions = incorporateResolutions(
						resolutions,
						new Resolution[] {
							fitEntry.getResolution()
						});
			}
		}
	}

	private static Resolution[] incorporateResolutions(
			final Resolution[] res1,
			final Resolution[] res2 ) {
		final TreeSet<Resolution> resolutionSet = new TreeSet<>();
		for (final Resolution res : res1) {
			resolutionSet.add(res);
		}
		for (final Resolution res : res2) {
			resolutionSet.add(res);
		}
		final Resolution[] combinedRes = new Resolution[resolutionSet.size()];
		int i = 0;
		for (final Resolution res : resolutionSet) {
			combinedRes[i++] = res;
		}
		return combinedRes;
	}

	@Override
	public void merge(
			final Mergeable statistics ) {
		if (statistics instanceof OverviewStatistics) {
			synchronized (this) {
				resolutions = incorporateResolutions(
						resolutions,
						((OverviewStatistics) statistics).getResolutions());
			}
		}
	}

	public Resolution[] getResolutions() {
		synchronized (this) {
			return resolutions;
		}
	}

	@Override
	public Resolution[] getResult() {
		return getResolutions();
	}

	@Override
	protected String resultsName() {
		return "resolutions";
	}

	@Override
	protected Object resultsValue() {
		final Map<Integer, double[]> map = new HashMap<>();
		synchronized (this) {
			for (int i = 0; i < resolutions.length; i++) {
				map.put(
						i,
						resolutions[i].getResolutionPerDimension());
			}
		}
		return map;
	}
}
