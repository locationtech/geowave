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
package mil.nga.giat.geowave.datastore.accumulo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.core.store.operations.MetadataType;

public class MergingCombiner extends
		Combiner
{
	// this is "columns" because it is mimicing the behavior of
	// org.apache.accumulo.core.iterators.Combiner.setColumns()
	private static final String COLUMNS_OPTION = "columns";

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		// the original may be unmodifiable so we need to create a modifiable
		// clone
		Map<String, String> modifiableOptions = new HashMap<>(
				options);
		modifiableOptions.put(
				COLUMNS_OPTION,
				getColumnOptionValue(options));
		super.init(
				source,
				modifiableOptions,
				env);
	}

	protected String getColumnOptionValue(
			final Map<String, String> options ) {
		// if this is not "row" merging than it is merging stats on the metadata
		// table
		return MetadataType.STATS.name();
	}

	@Override
	public Value reduce(
			final Key key,
			final Iterator<Value> iter ) {
		Mergeable currentMergeable = null;
		while (iter.hasNext()) {
			final Value val = iter.next();
			// hopefully its never the case that null stastics are stored,
			// but just in case, check
			final Mergeable mergeable = getMergeable(
					key,
					val.get());
			if (mergeable != null) {
				if (currentMergeable == null) {
					currentMergeable = mergeable;
				}
				else {
					currentMergeable.merge(mergeable);
				}
			}
		}
		if (currentMergeable != null) {
			return new Value(
					getBinary(currentMergeable));
		}
		return super.getTopValue();
	}

	protected Mergeable getMergeable(
			final Key key,
			final byte[] binary ) {
		return (Mergeable) AccumuloUtils.fromBinary(binary);
	}

	protected byte[] getBinary(
			final Mergeable mergeable ) {
		return AccumuloUtils.toBinary(mergeable);
	}
}
