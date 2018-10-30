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
package org.locationtech.geowave.datastore.dynamodb.operations;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bouncycastle.util.Arrays;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.operations.BaseReaderParams;
import org.locationtech.geowave.core.store.operations.ParallelDecoder;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.SimpleParallelDecoder;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBRow;
import org.locationtech.geowave.datastore.dynamodb.util.AsyncPaginatedQuery;
import org.locationtech.geowave.datastore.dynamodb.util.AsyncPaginatedScan;
import org.locationtech.geowave.datastore.dynamodb.util.DynamoDBUtils;
import org.locationtech.geowave.datastore.dynamodb.util.LazyPaginatedQuery;
import org.locationtech.geowave.datastore.dynamodb.util.LazyPaginatedScan;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DynamoDBReader<T> implements
		RowReader<T>
{
	private static final boolean ASYNC = false;
	private final ReaderParams<T> readerParams;
	private final RecordReaderParams<T> recordReaderParams;
	private final DynamoDBOperations operations;
	private Iterator<T> iterator;
	private final GeoWaveRowIteratorTransformer<T> rowTransformer;
	private Closeable closeable = null;

	private Predicate<GeoWaveRow> visibilityFilter;

	public DynamoDBReader(
			final ReaderParams<T> readerParams,
			final DynamoDBOperations operations ) {
		this.readerParams = readerParams;
		recordReaderParams = null;
		processAuthorizations(
				readerParams.getAdditionalAuthorizations(),
				readerParams);
		this.operations = operations;
		this.rowTransformer = readerParams.getRowTransformer();
		initScanner();
	}

	public DynamoDBReader(
			final RecordReaderParams<T> recordReaderParams,
			final DynamoDBOperations operations ) {
		readerParams = null;
		this.recordReaderParams = recordReaderParams;
		processAuthorizations(
				recordReaderParams.getAdditionalAuthorizations(),
				recordReaderParams);
		this.operations = operations;
		this.rowTransformer = recordReaderParams.getRowTransformer();

		initRecordScanner();
	}

	private void processAuthorizations(
			final String[] authorizations,
			final BaseReaderParams<T> params ) {
		visibilityFilter = new ClientVisibilityFilter(
				Sets.newHashSet(authorizations));
	}

	protected void initScanner() {
		final String tableName = operations
				.getQualifiedTableName(
						readerParams.getIndex().getName());

		// if ((readerParams.getLimit() != null) && (readerParams.getLimit() >
		// 0)) {
		// TODO: we should do something here
		// }

		final List<QueryRequest> requests = new ArrayList<>();

		final Collection<SinglePartitionQueryRanges> ranges = readerParams.getQueryRanges().getPartitionQueryRanges();

		if ((ranges != null) && !ranges.isEmpty()) {
			ranges
					.forEach(
							(queryRequest -> requests
									.addAll(
											addQueryRanges(
													tableName,
													queryRequest,
													readerParams.getAdapterIds(),
													readerParams.getInternalAdapterStore()))));

		}
		// else if ((readerParams.getAdapterIds() != null) &&
		// !readerParams.getAdapterIds().isEmpty()) {
		// //TODO this isn't going to work because there aren't partition keys
		// being passed along
		// requests.addAll(
		// getAdapterOnlyQueryRequests(
		// tableName,
		// readerParams.getAdapterIds()));
		// }

		startRead(
				requests,
				tableName,
				readerParams.getMaxResolutionSubsamplingPerDimension() == null);
	}

	protected void initRecordScanner() {
		final String tableName = operations.getQualifiedTableName(recordReaderParams.getIndex().getName());

		final ArrayList<Short> adapterIds = Lists.newArrayList();
		if ((recordReaderParams.getAdapterIds() != null) && (recordReaderParams.getAdapterIds().length > 0)) {
			for (final Short adapterId : recordReaderParams.getAdapterIds()) {
				adapterIds.add(adapterId);
			}
		}

		final List<QueryRequest> requests = new ArrayList<>();

		final GeoWaveRowRange range = recordReaderParams.getRowRange();
		for (final Short adapterId : adapterIds) {
			final ByteArray startKey = range.isInfiniteStartSortKey() ? null : new ByteArray(
					range.getStartSortKey());
			final ByteArray stopKey = range.isInfiniteStopSortKey() ? null : new ByteArray(
					range.getEndSortKey());
			requests.add(getQuery(
					tableName,
					range.getPartitionKey(),
					new ByteArrayRange(
							startKey,
							stopKey),
					adapterId));
		}
		startRead(
				requests,
				tableName,
				false);
	}

	private void startRead(
			final List<QueryRequest> requests,
			final String tableName,
			final boolean parallelDecode ) {
		Iterator<Map<String, AttributeValue>> rawIterator;
		Predicate<DynamoDBRow> adapterIdFilter = null;

		final Function<Iterator<Map<String, AttributeValue>>, Iterator<DynamoDBRow>> rawToDynamoDBRow = new Function<Iterator<Map<String, AttributeValue>>, Iterator<DynamoDBRow>>() {

			@Override
			public Iterator<DynamoDBRow> apply(
					final Iterator<Map<String, AttributeValue>> input ) {
				return new GeoWaveRowMergingIterator<>(
						Iterators
								.filter(
										Iterators
												.transform(
														input,
														new DynamoDBRow.GuavaRowTranslationHelper()),
										visibilityFilter));
			}

		};

		if (!requests.isEmpty()) {
			if (ASYNC) {
				rawIterator = Iterators
						.concat(
								requests
										.parallelStream()
										.map(
												this::executeAsyncQueryRequest)
										.iterator());
			}
			else {
				rawIterator = Iterators
						.concat(
								requests
										.parallelStream()
										.map(
												this::executeQueryRequest)
										.iterator());
			}
		}
		else {
			if (ASYNC) {
				final ScanRequest request = new ScanRequest(
						tableName);
				rawIterator = new AsyncPaginatedScan(
						request,
						operations.getClient());
			}
			else {
				// query everything
				final ScanRequest request = new ScanRequest(
						tableName);
				final ScanResult scanResult = operations
						.getClient()
						.scan(
								request);
				rawIterator = new LazyPaginatedScan(
						scanResult,
						request,
						operations.getClient());
				// TODO it'd be best to keep the set of partitions as a stat and
				// use it to query by adapter IDs server-side
				// but stats could be disabled so we may need to do client-side
				// filtering by adapter ID
				if ((readerParams.getAdapterIds() != null) && (readerParams.getAdapterIds().length > 0)) {
					adapterIdFilter = new Predicate<DynamoDBRow>() {

						@Override
						public boolean apply(
								final DynamoDBRow input ) {
							return Arrays
									.contains(
											readerParams.getAdapterIds(),
											input.getAdapterId());
						}

					};
				}
			}
		}

		Iterator<DynamoDBRow> rowIter = rawToDynamoDBRow
				.apply(
						rawIterator);
		if (adapterIdFilter != null) {
			rowIter = Iterators
					.filter(
							rowIter,
							adapterIdFilter);
		}
		if (parallelDecode) {
			final ParallelDecoder<T> decoder = new SimpleParallelDecoder<>(
					rowTransformer,
					Iterators
							.transform(
									rowIter,
									r -> (GeoWaveRow) r));
			try {
				decoder.startDecode();
			}
			catch (final Exception e) {
				Throwables
						.propagate(
								e);
			}
			iterator = decoder;
			closeable = decoder;
		}
		else {
			iterator = rowTransformer
					.apply(
							Iterators
									.transform(
											rowIter,
											r -> (GeoWaveRow) r));
			closeable = null;
		}
	}

	@Override
	public void close()
			throws Exception {
		if (closeable != null) {
			closeable.close();
			closeable = null;
		}
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public T next() {
		return iterator.next();
	}

	private List<QueryRequest> getAdapterOnlyQueryRequests(
			final String tableName,
			final ArrayList<Short> internalAdapterIds ) {
		final List<QueryRequest> allQueries = new ArrayList<>();

		for (final short internalAdapterId : internalAdapterIds) {
			final QueryRequest singleAdapterQuery = new QueryRequest(
					tableName);

			final byte[] start = ByteArrayUtils.shortToByteArray(internalAdapterId);
			final byte[] end = new ByteArray(
					start).getNextPrefix();
			singleAdapterQuery.addKeyConditionsEntry(
					DynamoDBRow.GW_RANGE_KEY,
					new Condition().withComparisonOperator(
							ComparisonOperator.BETWEEN).withAttributeValueList(
							new AttributeValue().withB(ByteBuffer.wrap(start)),
							new AttributeValue().withB(ByteBuffer.wrap(end))));

			allQueries.add(singleAdapterQuery);
		}

		return allQueries;
	}

	private QueryRequest getQuery(
			final String tableName,
			final byte[] partitionId,
			final ByteArrayRange sortRange,
			final short internalAdapterId ) {
		final byte[] start;
		final byte[] end;
		final QueryRequest query = new QueryRequest(
				tableName).addKeyConditionsEntry(
				DynamoDBRow.GW_PARTITION_ID_KEY,
				new Condition().withComparisonOperator(
						ComparisonOperator.EQ).withAttributeValueList(
						new AttributeValue().withB(ByteBuffer.wrap(partitionId))));
		if (sortRange == null) {
			start = ByteArrayUtils.shortToByteArray(internalAdapterId);
			end = new ByteArray(
					start).getNextPrefix();
		}
		else if (sortRange.isSingleValue()) {
			start = ByteArrayUtils.combineArrays(
					ByteArrayUtils.shortToByteArray(internalAdapterId),
					DynamoDBUtils.encodeSortableBase64(sortRange.getStart().getBytes()));
			end = ByteArrayUtils.combineArrays(
					ByteArrayUtils.shortToByteArray(internalAdapterId),
					DynamoDBUtils.encodeSortableBase64(sortRange.getStart().getNextPrefix()));
		}
		else {
			if (sortRange.getStart() == null) {
				start = ByteArrayUtils.shortToByteArray(internalAdapterId);
			}
			else {
				start = ByteArrayUtils.combineArrays(
						ByteArrayUtils.shortToByteArray(internalAdapterId),
						DynamoDBUtils.encodeSortableBase64(sortRange.getStart().getBytes()));
			}
			if (sortRange.getEnd() == null) {
				end = new ByteArray(
						ByteArrayUtils.shortToByteArray(internalAdapterId)).getNextPrefix();
			}
			else {
				end = ByteArrayUtils.combineArrays(
						ByteArrayUtils.shortToByteArray(internalAdapterId),
						DynamoDBUtils.encodeSortableBase64(sortRange.getEndAsNextPrefix().getBytes()));
			}
		}
		query.addKeyConditionsEntry(
				DynamoDBRow.GW_RANGE_KEY,
				new Condition().withComparisonOperator(
						ComparisonOperator.BETWEEN).withAttributeValueList(
						new AttributeValue().withB(ByteBuffer.wrap(start)),
						new AttributeValue().withB(ByteBuffer.wrap(end))));
		return query;
	}

	private List<QueryRequest> addQueryRanges(
			final String tableName,
			final SinglePartitionQueryRanges r,
			short[] adapterIds,
			final InternalAdapterStore adapterStore ) {
		final List<QueryRequest> retVal = new ArrayList<>();
		final ByteArray partitionKey = r.getPartitionKey();
		final byte[] partitionId = ((partitionKey == null) || (partitionKey.getBytes().length == 0))
				? DynamoDBWriter.EMPTY_PARTITION_KEY
				: partitionKey.getBytes();
		if (((adapterIds == null) || (adapterIds.length == 0)) && (adapterStore != null)) {
			adapterIds = adapterStore.getAdapterIds();
		}

		for (final Short adapterId : adapterIds) {
			final Collection<ByteArrayRange> sortKeyRanges = r.getSortKeyRanges();
			if ((sortKeyRanges != null) && !sortKeyRanges.isEmpty()) {
				sortKeyRanges
						.forEach(
								(sortKeyRange -> retVal
										.add(
												getQuery(
														tableName,
														partitionId,
														sortKeyRange,
														adapterId))));
			}
			else {
				retVal
						.add(
								getQuery(
										tableName,
										partitionId,
										null,
										adapterId));
			}
		}
		return retVal;
	}

	private Iterator<Map<String, AttributeValue>> executeQueryRequest(
			final QueryRequest queryRequest ) {
		final QueryResult result = operations.getClient().query(
				queryRequest);
		return new LazyPaginatedQuery(
				result,
				queryRequest,
				operations.getClient());
	}

	/**
	 * Asynchronous version of the query request. Does not block
	 */
	public Iterator<Map<String, AttributeValue>> executeAsyncQueryRequest(
			final QueryRequest queryRequest ) {
		return new AsyncPaginatedQuery(
				queryRequest,
				operations.getClient());
	}
}
