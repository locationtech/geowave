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
package org.locationtech.geowave.datastore.accumulo.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.UnreadFieldDataList;
import org.locationtech.geowave.core.store.flatten.FlattenedUnreadData;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This iterator wraps a DistributableQueryFilter which is deserialized from a
 * byte array passed as an option with a "filter" key. Also, the model is needed
 * to deserialize the row into a set of fields that can be used by the filter.
 * The model is deserialized from a byte array stored as an option with the key
 * "model". If either one of these serialized options are not successfully
 * found, this iterator will accept everything.
 */
public class WholeRowQueryFilterIterator extends
		WholeRowIterator
{
	private final static Logger LOGGER = LoggerFactory.getLogger(WholeRowQueryFilterIterator.class);
	protected QueryFilterIterator queryFilterIterator;

	@Override
	protected boolean filter(
			final Text currentRow,
			final List<Key> keys,
			final List<Value> values ) {
		if ((queryFilterIterator != null) && queryFilterIterator.isSet()) {
			final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
			final List<FlattenedUnreadData> unreadData = new ArrayList<>();
			for (int i = 0; (i < keys.size()) && (i < values.size()); i++) {
				final Key key = keys.get(i);
				final Value value = values.get(i);
				unreadData.add(queryFilterIterator.aggregateFieldData(
						key,
						value,
						commonData));
			}
			return queryFilterIterator.applyRowFilter(
					currentRow,
					commonData,
					unreadData.isEmpty() ? null : new UnreadFieldDataList(
							unreadData));
		}
		// if the query filter or index model did not get sent to this iterator,
		// it'll just have to accept everything
		return true;
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		queryFilterIterator = new QueryFilterIterator();
		queryFilterIterator.setOptions(options);
		super.init(
				source,
				options,
				env);
	}

}
