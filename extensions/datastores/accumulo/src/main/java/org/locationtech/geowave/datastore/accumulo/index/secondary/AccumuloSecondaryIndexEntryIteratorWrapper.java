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
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.util.SecondaryIndexEntryIteratorWrapper;
import org.locationtech.geowave.datastore.accumulo.util.AccumuloSecondaryIndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To be used when dealing with either a 'FULL' or 'PARTIAL' secondary index
 * type
 */
public class AccumuloSecondaryIndexEntryIteratorWrapper<T> extends
		SecondaryIndexEntryIteratorWrapper<T, T>
{

	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloSecondaryIndexEntryIteratorWrapper.class);
	private final Scanner scanner;
	private final Index index;

	public AccumuloSecondaryIndexEntryIteratorWrapper(
			final Scanner scanner,
			final InternalDataAdapter<T> adapter,
			final Index index ) {
		super(
				scanner.iterator(),
				adapter);
		this.scanner = scanner;
		this.index = index;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected T decodeRow(
			final Object row ) {
		Entry<Key, Value> entry = null;
		try {
			entry = (Entry<Key, Value>) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error("Row is not an accumulo row entry.");
			return null;
		}
		return AccumuloSecondaryIndexUtils.decodeRow(
				entry.getKey(),
				entry.getValue(),
				adapter,
				index);
	}

	@Override
	public void close() {
		scanner.close();
	}

}
