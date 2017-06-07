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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;

public class AccumuloRowIdsDelete<T> extends
		AccumuloRowIdsQuery<T>
{

	private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloRowIdsDelete.class);

	public AccumuloRowIdsDelete(
			DataAdapter adapter,
			PrimaryIndex index,
			Collection rows,
			ScanCallback scanCallback,
			DedupeFilter dedupFilter,
			String[] authorizations ) {
		super(
				adapter,
				index,
				rows,
				scanCallback,
				dedupFilter,
				authorizations);
	}

	@Override
	protected boolean useWholeRowIterator() {
		return false;
	}

	@Override
	protected CloseableIterator<Object> initCloseableIterator(
			ScannerBase scanner,
			Iterator it ) {
		return new CloseableIteratorWrapper(
				new Closeable() {
					boolean closed = false;

					@Override
					public void close()
							throws IOException {
						if (!closed) {
							if (scanner instanceof BatchDeleter) {
								try {
									((BatchDeleter) scanner).delete();
								}
								catch (MutationsRejectedException | TableNotFoundException e) {
									LOGGER.warn(
											"Unable to delete rows by query constraints",
											e);
								}
							}
							scanner.close();
						}
						closed = true;
					}
				},
				it);
	}

	@Override
	protected ScannerBase createScanner(
			AccumuloOperations accumuloOperations,
			String tableName,
			boolean batchScanner,
			String... authorizations )
			throws TableNotFoundException {
		BatchDeleter deleter = accumuloOperations.createBatchDeleter(
				tableName,
				authorizations);
		deleter.removeScanIterator(BatchDeleter.class.getName() + ".NOVALUE");
		return deleter;
	}

}
