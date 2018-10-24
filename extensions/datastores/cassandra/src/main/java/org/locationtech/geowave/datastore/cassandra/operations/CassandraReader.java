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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.bouncycastle.util.Arrays;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.datastore.cassandra.CassandraRow;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;

import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CassandraReader<T> implements
		RowReader<T>
{
	private final ReaderParams<T> readerParams;
	private final RecordReaderParams<T> recordReaderParams;
	private final CassandraOperations operations;
	private final GeoWaveRowIteratorTransformer<T> rowTransformer;

	private CloseableIterator<T> iterator;

	public CassandraReader(
			final ReaderParams<T> readerParams,
			final CassandraOperations operations ) {
		this.readerParams = readerParams;
		recordReaderParams = null;
		this.operations = operations;
		this.rowTransformer = readerParams.getRowTransformer();

		initScanner();
	}

	public CassandraReader(
			final RecordReaderParams<T> recordReaderParams,
			final CassandraOperations operations ) {
		readerParams = null;
		this.recordReaderParams = recordReaderParams;
		this.operations = operations;

		this.rowTransformer = recordReaderParams.getRowTransformer();

		initRecordScanner();
	}

	@SuppressWarnings("unchecked")
	private CloseableIterator<T> wrapResults(
			final CloseableIterator<CassandraRow> results,
			final Set<String> authorizations ) {
		return new CloseableIteratorWrapper<>(
				results,
				rowTransformer
						.apply((Iterator<GeoWaveRow>) (Iterator<? extends GeoWaveRow>) new GeoWaveRowMergingIterator<>(
								Iterators.filter(
										results,
										new ClientVisibilityFilter(
												authorizations)))));
	}

	protected void initScanner() {
		final Collection<SinglePartitionQueryRanges> ranges = readerParams.getQueryRanges().getPartitionQueryRanges();

		final Set<String> authorizations = Sets.newHashSet(readerParams.getAdditionalAuthorizations());
		if ((ranges != null) && !ranges.isEmpty()) {
			iterator = operations.getBatchedRangeRead(
					readerParams.getIndex().getName(),
					readerParams.getAdapterIds(),
					ranges,
					rowTransformer,
					new ClientVisibilityFilter(
							authorizations)).results();
		}
		else {
			// TODO figure out the query select by adapter IDs here
			final Select select = operations.getSelect(readerParams.getIndex().getName());
			CloseableIterator<CassandraRow> results = operations.executeQuery(select);
			if ((readerParams.getAdapterIds() != null) && (readerParams.getAdapterIds().length > 0)) {
				// TODO because we aren't filtering server-side by adapter ID,
				// we will need to filter here on the client
				results = new CloseableIteratorWrapper<>(
						results,
						Iterators.filter(
								results,
								new Predicate<CassandraRow>() {
									@Override
									public boolean apply(
											final CassandraRow input ) {
										return Arrays.contains(
												readerParams.getAdapterIds(),
												input.getAdapterId());
									}
								}));
			}
			iterator = wrapResults(
					results,
					authorizations);
		}

	}

	protected void initRecordScanner() {
		final short[] adapterIds = recordReaderParams.getAdapterIds() != null ? recordReaderParams.getAdapterIds()
				: new short[0];

		final GeoWaveRowRange range = recordReaderParams.getRowRange();
		final ByteArray startKey = range.isInfiniteStartSortKey() ? null : new ByteArray(
				range.getStartSortKey());
		final ByteArray stopKey = range.isInfiniteStopSortKey() ? null : new ByteArray(
				range.getEndSortKey());
		final SinglePartitionQueryRanges partitionRange = new SinglePartitionQueryRanges(
				new ByteArray(
						range.getPartitionKey()),
				Collections.singleton(new ByteArrayRange(
						startKey,
						stopKey)));
		final Set<String> authorizations = Sets.newHashSet(recordReaderParams.getAdditionalAuthorizations());
		iterator = operations.getBatchedRangeRead(
				recordReaderParams.getIndex().getName(),
				adapterIds,
				Collections.singleton(partitionRange),
				rowTransformer,
				new ClientVisibilityFilter(
						authorizations)).results();
	}

	@Override
	public void close()
			throws Exception {
		iterator.close();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public T next() {
		return iterator.next();
	}

}
