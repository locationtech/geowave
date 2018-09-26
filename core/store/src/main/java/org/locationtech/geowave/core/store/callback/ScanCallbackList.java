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
package org.locationtech.geowave.core.store.callback;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class ScanCallbackList<T, R extends GeoWaveRow> implements
		ScanCallback<T, R>,
		Closeable
{
	private final List<ScanCallback<T, R>> callbacks;
	private ReentrantLock lock;
	private static Object MUTEX = new Object();

	public ScanCallbackList(
			final List<ScanCallback<T, R>> callbacks ) {
		this.callbacks = callbacks;
	}

	public void addScanCallback(
			ScanCallback<T, R> callback ) {
		callbacks.add(callback);
		if (lock != null) {
			lock.unlock();
		}
	}

	public void waitUntilCallbackAdded() {
		// this waits until a callback is added before allowing entryScanned()
		// calls to proceed
		this.lock = new ReentrantLock();
		this.lock.lock();
	}

	@Override
	public void entryScanned(
			final T entry,
			final R rows ) {
		if (lock != null) {
			synchronized (MUTEX) {
				if (lock != null) {
					lock.lock();
					lock = null;
				}
			}
		}
		for (final ScanCallback<T, R> callback : callbacks) {
			callback.entryScanned(
					entry,
					rows);
		}
	}

	@Override
	public void close()
			throws IOException {
		for (final ScanCallback<T, R> callback : callbacks) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
	}

}
