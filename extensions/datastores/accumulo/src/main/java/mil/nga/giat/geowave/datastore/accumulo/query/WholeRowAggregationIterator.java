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
package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.datastore.accumulo.encoding.AccumuloUnreadDataList;

public class WholeRowAggregationIterator extends
		WholeRowQueryFilterIterator
{
	private AggregationIterator aggregationIterator;

	public WholeRowAggregationIterator() {
		super();
	}

	@Override
	protected boolean filter(
			final Text currentRow,
			final List<Key> keys,
			final List<Value> values ) {
		if ((aggregationIterator != null) && (aggregationIterator.queryFilterIterator != null)) {
			final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
			final List<FlattenedUnreadData> unreadData = new ArrayList<>();
			for (int i = 0; (i < keys.size()) && (i < values.size()); i++) {
				final Key key = keys.get(i);
				final Value value = values.get(i);
				final FlattenedUnreadData singleRow = aggregationIterator.queryFilterIterator.aggregateFieldData(
						key,
						value,
						commonData);
				if (singleRow != null) {
					unreadData.add(singleRow);
				}
			}
			final CommonIndexedPersistenceEncoding encoding = QueryFilterIterator.getEncoding(
					currentRow,
					commonData,
					unreadData.isEmpty() ? null : new AccumuloUnreadDataList(
							unreadData));
			boolean queryFilterResult = true;
			if (aggregationIterator.queryFilterIterator.isSet()) {
				queryFilterResult = aggregationIterator.queryFilterIterator.applyRowFilter(encoding);
			}
			if (queryFilterResult) {
				aggregationIterator.aggregateRow(
						currentRow,
						queryFilterIterator.model,
						encoding);
			}
		}
		// we don't want to return anything but the aggregation result
		return false;
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		aggregationIterator = new AggregationIterator();
		aggregationIterator.setParent(new WholeRowAggregationParent());
		aggregationIterator.setOptions(options);
		aggregationIterator.queryFilterIterator = new QueryFilterIterator();
		aggregationIterator.queryFilterIterator.setOptions(options);
		super.init(
				source,
				options,
				env);
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(
			final IteratorEnvironment env ) {
		final SortedKeyValueIterator<Key, Value> iterator = super.deepCopy(env);
		if (iterator instanceof WholeRowAggregationIterator) {
			aggregationIterator = new AggregationIterator();
			aggregationIterator.deepCopyIterator(((WholeRowAggregationIterator) iterator).aggregationIterator);
			aggregationIterator.setParent(new WholeRowAggregationParent());
		}
		return iterator;
	}

	@Override
	public Key getTopKey() {
		return aggregationIterator.getTopKey();
	}

	@Override
	public Value getTopValue() {
		return aggregationIterator.getTopValue();
	}

	@Override
	public boolean hasTop() {
		return aggregationIterator.hasTop();
	}

	@Override
	public void next()
			throws IOException {
		aggregationIterator.next();
	}

	@Override
	public void seek(
			final Range range,
			final Collection<ByteSequence> columnFamilies,
			final boolean inclusive )
			throws IOException {
		aggregationIterator.seek(
				range,
				columnFamilies,
				inclusive);
	}

	public class WholeRowAggregationParent implements
			SortedKeyValueIterator<Key, Value>
	{

		@Override
		public void init(
				final SortedKeyValueIterator<Key, Value> source,
				final Map<String, String> options,
				final IteratorEnvironment env )
				throws IOException {
			WholeRowAggregationIterator.super.init(
					source,
					options,
					env);
		}

		@Override
		public boolean hasTop() {
			return WholeRowAggregationIterator.super.hasTop();
		}

		@Override
		public void next()
				throws IOException {
			WholeRowAggregationIterator.super.next();
		}

		@Override
		public void seek(
				final Range range,
				final Collection<ByteSequence> columnFamilies,
				final boolean inclusive )
				throws IOException {
			WholeRowAggregationIterator.super.seek(
					range,
					columnFamilies,
					inclusive);
		}

		@Override
		public Key getTopKey() {
			return WholeRowAggregationIterator.super.getTopKey();
		}

		@Override
		public Value getTopValue() {
			return WholeRowAggregationIterator.super.getTopValue();
		}

		@Override
		public SortedKeyValueIterator<Key, Value> deepCopy(
				final IteratorEnvironment env ) {
			return WholeRowAggregationIterator.super.deepCopy(env);
		}

	}
}
