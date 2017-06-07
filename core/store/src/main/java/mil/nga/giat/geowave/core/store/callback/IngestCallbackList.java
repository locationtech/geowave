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
package mil.nga.giat.geowave.core.store.callback;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;

public class IngestCallbackList<T> implements
		IngestCallback<T>,
		Flushable,
		Closeable
{
	private final List<IngestCallback<T>> callbacks;

	public IngestCallbackList(
			final List<IngestCallback<T>> callbacks ) {
		this.callbacks = callbacks;
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final IngestCallback<T> callback : callbacks) {
			callback.entryIngested(
					entryInfo,
					entry);
		}
	}

	@Override
	public void close()
			throws IOException {
		for (final IngestCallback<T> callback : callbacks) {
			if (callback instanceof Closeable) ((Closeable) callback).close();
		}
	}

	@Override
	public void flush()
			throws IOException {
		for (final IngestCallback<T> callback : callbacks) {
			if (callback instanceof Flushable) ((Flushable) callback).flush();
		}
	}

}
