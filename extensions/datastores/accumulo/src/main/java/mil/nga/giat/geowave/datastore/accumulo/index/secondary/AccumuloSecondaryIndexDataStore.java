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
package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.BaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexUtils;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;

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
	protected Writer getWriter(
			final ByteArrayId secondaryIndexId ) {
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
	protected GeoWaveRow buildJoinDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexRowId ) {
		// final Mutation m = new Mutation(
		// secondaryIndexRowId);
		// m.putDelete(
		// SecondaryIndexUtils.constructColumnFamily(
		// adapterId,
		// indexedAttributeFieldId),
		// SecondaryIndexUtils.constructColumnQualifier(
		// primaryIndexId,
		// primaryIndexRowId));
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
			final SecondaryIndex<T> secondaryIndex,
			final ByteArrayId indexedAttributeFieldId,
			final InternalDataAdapter<T> adapter,
			final PrimaryIndex primaryIndex,
			final DistributableQuery query,
			final String... authorizations ) {
		final Scanner scanner = getScanner(
				StringUtils.stringFromBinary(secondaryIndex.getId().getBytes()),
				authorizations);

		if (scanner != null) {
			scanner.fetchColumnFamily(new Text(
					SecondaryIndexUtils.constructColumnFamily(
							adapter.getAdapterId(),
							indexedAttributeFieldId)));
			final Collection<Range> ranges = getScanRanges(query.getSecondaryIndexConstraints(secondaryIndex));
			for (final Range range : ranges) {
				scanner.setRange(range);
			}

			if (!secondaryIndex.getSecondaryIndexType().equals(
					SecondaryIndexType.JOIN)) {
				final IteratorSetting iteratorSettings = new IteratorSetting(
						10,
						"GEOWAVE_WHOLE_ROW_ITERATOR",
						WholeRowIterator.class);
				scanner.addScanIterator(iteratorSettings);
				return new AccumuloSecondaryIndexEntryIteratorWrapper<T>(
						scanner,
						adapter,
						primaryIndex);
			}
			else {
				final List<CloseableIterator<Object>> allResults = new ArrayList<>();
				try (final CloseableIterator<Pair<ByteArrayId, ByteArrayId>> joinEntryIterator = new AccumuloSecondaryIndexJoinEntryIteratorWrapper<T>(
						scanner,
						adapter)) {
					while (joinEntryIterator.hasNext()) {
						final Pair<ByteArrayId, ByteArrayId> entry = joinEntryIterator.next();
						final ByteArrayId primaryIndexId = entry.getLeft();
						final ByteArrayId primaryIndexRowId = entry.getRight();
						final CloseableIterator<Object> intermediateResults = dataStore.query(
								new QueryOptions(
										adapter.getAdapterId(),
										primaryIndexId),
								new PrefixIdQuery(
										null, // TODO GEOWAVE-1018: need
												// partition key with join
												// entry, also why is the a
												// prefix query and not an
												// insertion ID query
										primaryIndexRowId));
						allResults.add(intermediateResults);
						return new CloseableIteratorWrapper<T>(
								new Closeable() {
									@Override
									public void close()
											throws IOException {
										for (final CloseableIterator<Object> resultIter : allResults) {
											resultIter.close();
										}
									}
								},
								(Iterator<T>) Iterators.concat(allResults.iterator()));
					}
				}
				catch (final IOException e) {
					LOGGER.error(
							"Could not close iterator",
							e);
				}
			}
		}

		return new CloseableIterator.Empty<T>();
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
