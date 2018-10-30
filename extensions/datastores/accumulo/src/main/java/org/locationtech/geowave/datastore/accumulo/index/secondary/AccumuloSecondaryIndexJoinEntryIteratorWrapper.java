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

import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.index.SecondaryIndexUtils;
import org.locationtech.geowave.core.store.util.SecondaryIndexEntryIteratorWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To be used when dealing with a 'JOIN' secondary index type
 */
public class AccumuloSecondaryIndexJoinEntryIteratorWrapper<T> extends
		SecondaryIndexEntryIteratorWrapper<T, Pair<String, ByteArray>>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloSecondaryIndexJoinEntryIteratorWrapper.class);
	private final Scanner scanner;

	public AccumuloSecondaryIndexJoinEntryIteratorWrapper(
			final Scanner scanner,
			final InternalDataAdapter<T> adapter ) {
		super(
				scanner.iterator(),
				adapter);
		this.scanner = scanner;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Pair<String, ByteArray> decodeRow(
			final Object row ) {
		Entry<Key, Value> entry = null;
		try {
			entry = (Entry<Key, Value>) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error("Row is not an accumulo row entry.");
			return null;
		}
		final byte[] cqBytes = entry.getKey().getColumnQualifierData().getBackingArray();
		return Pair.of(
				SecondaryIndexUtils.getIndexName(cqBytes),
				SecondaryIndexUtils.getPrimaryRowId(cqBytes));
	}

	@Override
	public void close() {
		scanner.close();
	}

}
