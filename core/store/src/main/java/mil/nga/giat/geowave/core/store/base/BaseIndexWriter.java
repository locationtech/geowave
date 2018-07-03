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
package mil.nga.giat.geowave.core.store.base;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.IntermediaryWriteEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.InternalAdapterStoreImpl;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

class BaseIndexWriter<T> implements
		IndexWriter<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BaseIndexWriter.class);
	protected final PrimaryIndex index;
	protected final DataStoreOperations operations;
	protected final DataStoreOptions options;
	protected final IngestCallback<T> callback;
	protected Writer writer;

	protected final InternalDataAdapter<T> adapter;
	// protected final byte[] adapterId;
	final Closeable closable;

	public BaseIndexWriter(
			final InternalDataAdapter<T> adapter,
			final PrimaryIndex index,
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
	public PrimaryIndex[] getIndices() {
		return new PrimaryIndex[] {
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
			for (final FieldInfo field : ingestInfo.getFieldInfo()) {
				if ((field.getVisibility() != null) && (field.getVisibility().length > 0)) {
					if (!operations.insureAuthorizations(
							null,
							StringUtils.stringFromBinary(field.getVisibility()))) {
						LOGGER.error("Unable to set authorizations for ingested visibility '"
								+ StringUtils.stringFromBinary(field.getVisibility()) + "'");
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
						index.getId(),
						adapter.getInternalAdapterId());
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to open writer",
						e);
			}
		}
	}
}
