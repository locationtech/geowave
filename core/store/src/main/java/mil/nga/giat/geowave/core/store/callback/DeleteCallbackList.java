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
package mil.nga.giat.geowave.core.store.callback;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public class DeleteCallbackList<T, R extends GeoWaveRow> implements
		DeleteCallback<T, R>,
		Closeable
{
	private final List<DeleteCallback<T, R>> callbacks;

	public DeleteCallbackList(
			final List<DeleteCallback<T, R>> callbacks ) {
		this.callbacks = callbacks;
	}

	@Override
	public void entryDeleted(
			final T entry,
			final R... rows ) {
		for (final DeleteCallback<T, R> callback : callbacks) {
			callback.entryDeleted(
					entry,
					rows);
		}
	}

	@Override
	public void close()
			throws IOException {
		for (final DeleteCallback<T, R> callback : callbacks) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
	}

}
