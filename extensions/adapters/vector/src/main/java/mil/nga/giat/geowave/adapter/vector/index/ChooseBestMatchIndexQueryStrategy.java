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
package mil.nga.giat.geowave.adapter.vector.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.base.BaseDataStoreUtils;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class ChooseBestMatchIndexQueryStrategy implements
		IndexQueryStrategySPI
{
	public static final String NAME = "Best Match";
	private final static Logger LOGGER = LoggerFactory.getLogger(ChooseBestMatchIndexQueryStrategy.class);

	@Override
	public String toString() {
		return NAME;
	}

	@Override
	public CloseableIterator<Index<?, ?>> getIndices(
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
			final BasicQuery query,
			final PrimaryIndex[] indices ) {
		return new CloseableIterator<Index<?, ?>>() {
			PrimaryIndex nextIdx = null;
			boolean done = false;
			int i = 0;

			@Override
			public boolean hasNext() {
				long min = Long.MAX_VALUE;
				PrimaryIndex bestIdx = null;

				while (!done && (i < indices.length)) {
					nextIdx = indices[i++];
					if (nextIdx.getIndexStrategy().getOrderedDimensionDefinitions().length == 0) {
						continue;
					}
					final List<MultiDimensionalNumericData> constraints = query.getIndexConstraints(nextIdx);
					boolean containsRowRangeHistograms = false;
					for (final ByteArrayId statsId : stats.keySet()) {
						// find out if any partition histograms exist for this
						// index ID by checking the prefix
						if (statsId.getString().startsWith(
								RowRangeHistogramStatistics.composeId(
										nextIdx.getId(),
										null).getString())) {
							containsRowRangeHistograms = true;
							break;
						}
					}
					if (!containsRowRangeHistograms) {
						LOGGER
								.warn("Best Match Heuristic requires statistic RowRangeHistogramStatistics for each index to properly choose an index.");
					}

					if (IndexUtils.isFullTableScan(constraints)) {
						// keep this is as a default in case all indices
						// result in a full table scan
						if (bestIdx == null) {
							bestIdx = nextIdx;
						}
					}
					else {
						final QueryRanges ranges = DataStoreUtils.constraintsToQueryRanges(
								constraints,
								nextIdx.getIndexStrategy(),
								BaseDataStoreUtils.MAX_RANGE_DECOMPOSITION);
						final long temp = DataStoreUtils.cardinality(
								nextIdx,
								stats,
								ranges);
						if (temp < min) {
							bestIdx = nextIdx;
							min = temp;
						}
					}

				}
				nextIdx = bestIdx;
				done = true;
				return nextIdx != null;
			}

			@Override
			public Index<?, ?> next()
					throws NoSuchElementException {
				if (nextIdx == null) {
					throw new NoSuchElementException();
				}
				final Index<?, ?> returnVal = nextIdx;
				nextIdx = null;
				return returnVal;
			}

			@Override
			public void remove() {}

			@Override
			public void close()
					throws IOException {}
		};
	}
}
