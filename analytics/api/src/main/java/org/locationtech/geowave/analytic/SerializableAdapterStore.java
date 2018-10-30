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
package org.locationtech.geowave.analytic;

import java.io.IOException;
import java.io.Serializable;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Support for adapter stores that are Serializable. Rather than for an adapter
 * store to serialize its state, wrap an adapter store. If the adapter store is
 * not serializable, then log a warning message upon serialization.
 *
 *
 */

public class SerializableAdapterStore implements
		TransientAdapterStore,
		Serializable
{

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	final static Logger LOGGER = LoggerFactory.getLogger(SerializableAdapterStore.class);

	transient TransientAdapterStore adapterStore;

	public SerializableAdapterStore() {

	}

	public SerializableAdapterStore(
			final TransientAdapterStore adapterStore ) {
		super();
		this.adapterStore = adapterStore;
	}

	private TransientAdapterStore getAdapterStore() {
		if (adapterStore == null) {
			throw new IllegalStateException(
					"AdapterStore has not been initialized");
		}
		return adapterStore;
	}

	@Override
	public void addAdapter(
			final DataTypeAdapter<?> adapter ) {
		getAdapterStore().addAdapter(
				adapter);
	}

	@Override
	public DataTypeAdapter<?> getAdapter(
			final String typeName ) {
		return getAdapterStore().getAdapter(
				typeName);
	}

	@Override
	public boolean adapterExists(
			final String typeName ) {
		return getAdapterStore().adapterExists(
				typeName);
	}

	@Override
	public CloseableIterator<DataTypeAdapter<?>> getAdapters() {
		return getAdapterStore().getAdapters();
	}

	@Override
	public void removeAll() {
		getAdapterStore().removeAll();
	}

	private void writeObject(
			final java.io.ObjectOutputStream out )
			throws IOException {
		if (adapterStore instanceof Serializable) {
			out.writeBoolean(true);
			out.writeObject(adapterStore);
		}
		else {
			out.writeBoolean(false);
		}
	}

	private void readObject(
			final java.io.ObjectInputStream in )
			throws IOException,
			ClassNotFoundException {
		if (in.readBoolean()) {
			adapterStore = (TransientAdapterStore) in.readObject();
		}
		else {
			LOGGER.warn("Unable to initialized AdapterStore; the store is not serializable");
		}
	}

	@Override
	public void removeAdapter(
			final String typeName ) {
		getAdapterStore().removeAdapter(
				typeName);
	}
}
