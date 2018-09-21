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
package org.locationtech.geowave.adapter.vector.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.IndexUtils;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	public CloseableIterator<Index> getIndices(
			final Map<ByteArrayId, InternalDataStatistics<SimpleFeature>> stats,
			final BasicQuery query,
			final Index[] indices,
			final Map<QueryHint, Object> hints ) {
		return new CloseableIterator<Index>() {
			Index nextIdx = null;
			boolean done = false;
			int i = 0;

			@Override
			public boolean hasNext() {
				long min = Long.MAX_VALUE;
				Index bestIdx = null;

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
						final int maxRangeDecomposition;
						if (hints.containsKey(QueryHint.MAX_RANGE_DECOMPOSITION)) {
							maxRangeDecomposition = (Integer) hints.get(QueryHint.MAX_RANGE_DECOMPOSITION);
						}
						else {
							LOGGER
									.warn("No max range decomposition hint was provided, this should be provided from the data store options");
							maxRangeDecomposition = 2000;
						}
						final QueryRanges ranges = DataStoreUtils.constraintsToQueryRanges(
								constraints,
								nextIdx.getIndexStrategy(),
								maxRangeDecomposition);
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
			public Index next()
					throws NoSuchElementException {
				if (nextIdx == null) {
					throw new NoSuchElementException();
				}
				final Index returnVal = nextIdx;
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
