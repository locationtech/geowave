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
package org.locationtech.geowave.datastore.accumulo;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.accumulo.util.AccumuloUtils;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class MergingVisibilityCombiner extends
		TransformingIterator
{
	private static final byte[] AMPRISAND = StringUtils.stringToBinary("&");

	private ColumnSet combiners;
	private final Key workKey = new Key();

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		super.init(
				source,
				options,
				env);
		final String encodedColumns = getColumnOptionValue(options);
		if (encodedColumns.length() == 0) {
			throw new IllegalArgumentException(
					"The column must not be empty");
		}
		combiners = new ColumnSet(
				Lists.newArrayList(Splitter.on(
						",").split(
						encodedColumns)));
	}

	protected String getColumnOptionValue(
			final Map<String, String> options ) {
		// if this is not "row" merging than it is merging stats on the metadata
		// table
		return MetadataType.STATS.name();
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(
			final IteratorEnvironment env ) {
		final SortedKeyValueIterator<Key, Value> retVal = super.deepCopy(env);
		if (retVal instanceof MergingVisibilityCombiner) {
			((MergingVisibilityCombiner) retVal).combiners = combiners;
		}
		return retVal;
	}

	@Override
	protected PartialKey getKeyPrefix() {
		return PartialKey.ROW_COLFAM_COLQUAL;
	}

	@Override
	protected void transformRange(
			final SortedKeyValueIterator<Key, Value> input,
			final KVBuffer output )
			throws IOException {
		Mergeable currentMergeable = null;
		Key outputKey = null;
		workKey.set(input.getTopKey());
		// default to not combining, only combine when combiners does not
		// contain this column
		if ((combiners == null) || !combiners.contains(workKey) || workKey.isDeleted()) {
			// don't transform at all
			while (input.hasTop()) {
				output.append(
						input.getTopKey(),
						input.getTopValue());
				input.next();
			}
			return;
		}
		while (input.hasTop()) {
			final Value val = input.getTopValue();
			// the SortedKeyValueIterator uses the same instance of topKey to
			// hold keys (a wrapper)
			final Key currentKey = new Key(
					input.getTopKey());
			if (outputKey == null) {
				outputKey = currentKey;
			}
			else if ((currentMergeable != null) && !outputKey.getRowData().equals(
					currentKey.getRowData())) {
				output.append(
						outputKey,
						new Value(
								URLClassloaderUtils.toBinary(currentMergeable)));
				currentMergeable = null;
				outputKey = currentKey;
				continue;
			}
			else {
				final Text combinedVisibility = new Text(
						combineVisibilities(
								currentKey.getColumnVisibility().getBytes(),
								outputKey.getColumnVisibility().getBytes()));
				outputKey = replaceColumnVisibility(
						outputKey,
						combinedVisibility);
			}
			final Mergeable mergeable = getMergeable(
					currentKey,
					val.get());
			// hopefully its never the case that null mergeables are stored,
			// but just in case, check
			if (mergeable != null) {
				if (currentMergeable == null) {
					currentMergeable = mergeable;
				}
				else {
					currentMergeable.merge(mergeable);
				}
			}
			input.next();
		}
		if (currentMergeable != null) {
			output.append(
					outputKey,
					new Value(
							getBinary(currentMergeable)));
		}
	}

	protected Mergeable getMergeable(
			final Key key,
			final byte[] binary ) {
		return (Mergeable) URLClassloaderUtils.fromBinary(binary);
	}

	protected byte[] getBinary(
			final Mergeable mergeable ) {
		return URLClassloaderUtils.toBinary(mergeable);
	}

	private static byte[] combineVisibilities(
			final byte[] vis1,
			final byte[] vis2 ) {
		if ((vis1 == null) || (vis1.length == 0)) {
			return vis2;
		}
		if ((vis2 == null) || (vis2.length == 0)) {
			return vis1;
		}
		return new ColumnVisibility(
				ArrayUtils.addAll(
						ArrayUtils.addAll(
								ColumnVisibility.quote(vis1),
								AMPRISAND),
						ColumnVisibility.quote(vis2))).flatten();
	}

}
