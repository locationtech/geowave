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
package org.locationtech.geowave.datastore.accumulo.index.secondary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.index.BaseSecondaryIndexDataStore;
import org.locationtech.geowave.core.store.index.SecondaryIndexImpl;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloSecondaryIndexDataStore extends
		BaseSecondaryIndexDataStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloSecondaryIndexDataStore.class);
	private final AccumuloOperations accumuloOperations;
	private final AccumuloOptions accumuloOptions;
	private DataStore dataStore = null;

	public AccumuloSecondaryIndexDataStore(
			final AccumuloOperations accumuloOperations ) {
		this(
				accumuloOperations,
				new AccumuloOptions());
	}

	public AccumuloSecondaryIndexDataStore(
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		super();
		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
	}

	@Override
	public void setDataStore(
			final DataStore dataStore ) {
		this.dataStore = dataStore;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RowWriter getWriter(
			final String secondaryIndexName ) {
		// final String secondaryIndexName = secondaryIndexId.getString();
		// if (writerCache.containsKey(secondaryIndexName)) {
		// return writerCache.get(secondaryIndexName);
		// }
		// Writer writer = null;
		// try {
		// writer = accumuloOperations.createWriter(
		// secondaryIndexName,
		// true,
		// false,
		// accumuloOptions.isEnableBlockCache(),
		// null);
		// writerCache.put(
		// secondaryIndexName,
		// writer);
		// }
		// catch (final TableNotFoundException e) {
		// LOGGER.error(
		// "Error creating writer",
		// e);
		// }
		// return writer;
		return null;
	}

	@Override
	protected GeoWaveRow buildJoinMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexPartitionKey,
			final byte[] primaryIndexSortKey,
			final byte[] attributeVisibility ) {
		// final Mutation m = new Mutation(
		// secondaryIndexRowId);
		// final ColumnVisibility columnVisibility = new ColumnVisibility(
		// attributeVisibility);
		// m.put(
		// SecondaryIndexUtils.constructColumnFamily(
		// adapterId,
		// indexedAttributeFieldId),
		// SecondaryIndexUtils.constructColumnQualifier(
		// primaryIndexId,
		// primaryIndexPartitionKey),
		// columnVisibility,
		// EMPTY_VALUE);
		// return m;
		return null;
	}

	@Override
	protected GeoWaveRow buildMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] dataId,
			final byte[] fieldId,
			final byte[] fieldValue,
			final byte[] fieldVisibility ) {
		// final Mutation m = new Mutation(
		// secondaryIndexRowId);
		// final ColumnVisibility columnVisibility = new ColumnVisibility(
		// fieldVisibility);
		// m.put(
		// SecondaryIndexUtils.constructColumnFamily(
		// adapterId,
		// indexedAttributeFieldId),
		// SecondaryIndexUtils.constructColumnQualifier(
		// fieldId,
		// dataId),
		// columnVisibility,
		// fieldValue);
		// return m;
		return null;
	}

	@Override
	protected GeoWaveRow buildFullDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] dataId,
			final byte[] fieldId ) {
		// final Mutation m = new Mutation(
		// secondaryIndexRowId);
		// m.putDelete(
		// SecondaryIndexUtils.constructColumnFamily(
		// adapterId,
		// indexedAttributeFieldId),
		// SecondaryIndexUtils.constructColumnQualifier(
		// fieldId,
		// dataId));
		// return m;
		return null;
	}

	@Override
	public <T> CloseableIterator<T> query(
			final SecondaryIndexImpl<T> secondaryIndex,
			final String indexedAttributeFieldName,
			final InternalDataAdapter<T> adapter,
			final Index primaryIndex,
			final QueryConstraints query,
			final String... authorizations ) {
		// final Scanner scanner = getScanner(
		// secondaryIndex.getName(),
		// authorizations);
		//
		// if (scanner != null) {
		// scanner
		// .fetchColumnFamily(
		// new Text(
		// SecondaryIndexUtils
		// .constructColumnFamily(
		// adapter.getTypeName(),
		// indexedAttributeFieldName)));
		// final Collection<Range> ranges = getScanRanges(
		// query
		// .getSecondaryIndexConstraints(
		// secondaryIndex));
		// for (final Range range : ranges) {
		// scanner
		// .setRange(
		// range);
		// }
		//
		// if (!secondaryIndex
		// .getSecondaryIndexType()
		// .equals(
		// SecondaryIndexType.JOIN)) {
		// final IteratorSetting iteratorSettings = new IteratorSetting(
		// 10,
		// "GEOWAVE_WHOLE_ROW_ITERATOR",
		// WholeRowIterator.class);
		// scanner
		// .addScanIterator(
		// iteratorSettings);
		// return new AccumuloSecondaryIndexEntryIteratorWrapper<>(
		// scanner,
		// adapter,
		// primaryIndex);
		// }
		// else {
		// final List<CloseableIterator<Object>> allResults = new ArrayList<>();
		// try (final CloseableIterator<Pair<String, ByteArrayId>>
		// joinEntryIterator = new
		// AccumuloSecondaryIndexJoinEntryIteratorWrapper<>(
		// scanner,
		// adapter)) {
		// while (joinEntryIterator.hasNext()) {
		// final Pair<String, ByteArrayId> entry = joinEntryIterator.next();
		// final String primaryIndexName = entry.getLeft();
		// final ByteArrayId primaryIndexRowId = entry.getRight();
		// final QueryBuilder<Object, ?> bldr = QueryBuilder.newBuilder();
		// // TODO GEOWAVE-1018: need partition key with join
		// // entry, also why is the a prefix query and not an
		// // insertion ID query, sortKeyPrefix))
		// final CloseableIterator<Object> intermediateResults = dataStore
		// .query(
		// bldr
		// .addTypeName(
		//
		// adapter.getTypeName())
		// .indexName(
		// primaryIndexName)
		// .constraints(
		// bldr
		// .constraintsFactory()
		// .prefix(
		// null,
		// primaryIndexRowId))
		// .build());
		// allResults
		// .add(
		// intermediateResults);
		// return new CloseableIteratorWrapper<>(
		// new Closeable() {
		// @Override
		// public void close()
		// throws IOException {
		// for (final CloseableIterator<Object> resultIter : allResults) {
		// resultIter.close();
		// }
		// }
		// },
		// (Iterator<T>) Iterators
		// .concat(
		// allResults.iterator()));
		// }
		// }
		// }
		// }

		return new CloseableIterator.Empty<>();
	}

	private Scanner getScanner(
			final String secondaryIndexId,
			final String... visibility ) {
		Scanner scanner = null;
		try {
			scanner = accumuloOperations.createScanner(
					secondaryIndexId,
					visibility);
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Could not obtain batch scanner",
					e);
		}
		return scanner;
	}

	private Collection<Range> getScanRanges(
			final List<ByteArrayRange> ranges ) {
		if ((ranges == null) || ranges.isEmpty()) {
			return Collections.singleton(new Range());
		}
		final Collection<Range> scanRanges = new ArrayList<>();
		for (final ByteArrayRange range : ranges) {
			scanRanges.add(new Range(
					new Text(
							range.getStart().getBytes()),
					new Text(
							range.getEnd().getBytes())));
		}
		return scanRanges;
	}

	@Override
	protected GeoWaveRow buildJoinDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexPartitionKey,
			final byte[] primaryIndexSortKey )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	// private IteratorSetting getScanIteratorSettings(
	// final List<DistributableQueryFilter> distributableFilters,
	// final ByteArrayId primaryIndexId ) {
	// final IteratorSetting iteratorSettings = new IteratorSetting(
	// SecondaryIndexQueryFilterIterator.ITERATOR_PRIORITY,
	// SecondaryIndexQueryFilterIterator.ITERATOR_NAME,
	// SecondaryIndexQueryFilterIterator.class);
	// DistributableQueryFilter filter = getFilter(distributableFilters);
	// if (filter != null) {
	// iteratorSettings.addOption(
	// SecondaryIndexQueryFilterIterator.FILTERS,
	// ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(filter)));
	//
	// }
	// iteratorSettings.addOption(
	// SecondaryIndexQueryFilterIterator.PRIMARY_INDEX_ID,
	// primaryIndexId.getString());
	// return iteratorSettings;
	// }

}
