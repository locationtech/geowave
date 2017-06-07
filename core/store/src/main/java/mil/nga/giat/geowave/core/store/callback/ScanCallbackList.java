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
import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;

public class ScanCallbackList<T> implements
		ScanCallback<T>,
		Closeable
{
	private final List<ScanCallback<T>> callbacks;

	public ScanCallbackList(
			final List<ScanCallback<T>> callbacks ) {
		this.callbacks = callbacks;
	}

	@Override
	public void entryScanned(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final ScanCallback<T> callback : callbacks) {
			callback.entryScanned(
					entryInfo,
					entry);
		}
	}

	@Override
	public void close()
			throws IOException {
		for (final ScanCallback<T> callback : callbacks) {
			if (callback instanceof Closeable) ((Closeable) callback).close();
		}
	}

}
