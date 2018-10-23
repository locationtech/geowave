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
package org.locationtech.geowave.core.store.base;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BaseIndexWriter<T> implements
		Writer<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BaseIndexWriter.class);
	protected final Index index;
	protected final DataStoreOperations operations;
	protected final DataStoreOptions options;
	protected final IngestCallback<T> callback;
	protected RowWriter writer;

	protected final InternalDataAdapter<T> adapter;
	final Closeable closable;

	public BaseIndexWriter(
			final InternalDataAdapter<T> adapter,
			final Index index,
			final DataStoreOperations operations,
			final DataStoreOptions options,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		this.operations = operations;
		this.options = options;
		this.index = index;
		this.callback = callback;
		this.adapter = adapter;
		this.closable = closable;
	}

	@Override
	public Index[] getIndices() {
		return new Index[] {
			index
		};
	}

	@Override
	public InsertionIds write(
			final T entry ) {
		return write(
				entry,
				DataStoreUtils.UNCONSTRAINED_VISIBILITY);
	}

	@Override
	public InsertionIds write(
			final T entry,
			final VisibilityWriter<T> fieldVisibilityWriter ) {
		IntermediaryWriteEntryInfo entryInfo;
		synchronized (this) {
			ensureOpen();

			if (writer == null) {
				LOGGER.error("Null writer - empty list returned");
				return new InsertionIds();
			}
			entryInfo = BaseDataStoreUtils.getWriteInfo(
					entry,
					adapter,
					index,
					fieldVisibilityWriter);
			verifyVisibility(
					fieldVisibilityWriter,
					entryInfo);
			final GeoWaveRow[] rows = entryInfo.getRows();

			writer.write(rows);
			callback.entryIngested(
					entry,
					rows);
		}
		return entryInfo.getInsertionIds();
	}

	@Override
	public void close() {
		try {
			closable.close();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Cannot close callbacks",
					e);
		}
		// thread safe close
		closeInternal();
	}

	@Override
	public synchronized void flush() {
		// thread safe flush of the writers
		if (writer != null) {
			writer.flush();
		}
		if (this.callback instanceof Flushable) {
			try {
				((Flushable) callback).flush();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Cannot flush callbacks",
						e);
			}
		}
	}

	private void verifyVisibility(
			final VisibilityWriter customFieldVisibilityWriter,
			final IntermediaryWriteEntryInfo ingestInfo ) {
		if (customFieldVisibilityWriter != DataStoreUtils.UNCONSTRAINED_VISIBILITY) {
			for (final GeoWaveValue value : ingestInfo.getValues()) {
				if ((value.getVisibility() != null) && (value.getVisibility().length > 0)) {
					if (!operations.ensureAuthorizations(
							null,
							StringUtils.stringFromBinary(value.getVisibility()))) {
						LOGGER.error("Unable to set authorizations for ingested visibility '"
								+ StringUtils.stringFromBinary(value.getVisibility()) + "'");
					}

				}
			}
		}
	}

	protected synchronized void closeInternal() {
		if (writer != null) {
			try {
				writer.close();
				writer = null;
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to close writer",
						e);
			}
		}
	}

	protected synchronized void ensureOpen() {
		if (writer == null) {
			try {
				writer = operations.createWriter(
						index,
						adapter);
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to open writer",
						e);
			}
		}
	}
}
