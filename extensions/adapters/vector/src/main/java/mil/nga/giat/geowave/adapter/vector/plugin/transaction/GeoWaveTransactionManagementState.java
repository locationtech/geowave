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
package mil.nga.giat.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import mil.nga.giat.geowave.adapter.vector.plugin.lock.LockingManagement;

import org.geotools.data.DataSourceException;
import org.geotools.data.Transaction;

/**
 * Implements the transaction state protocol with Geotools.
 * 
 */
public class GeoWaveTransactionManagementState implements
		GeoWaveTransactionState
{

	private final GeoWaveDataStoreComponents components;
	private final LockingManagement lockingManager;
	private Transaction transaction;
	private final String txID;
	private final int transactionBufferSize;
	/**
	 * Map of differences by typeName.
	 * 
	 * <p>
	 * Differences are stored as a Map of Feature by fid, and are reset during a
	 * commit() or rollback().
	 * </p>
	 */
	private final Map<String, GeoWaveTransactionManagement> typeNameDiff = new HashMap<String, GeoWaveTransactionManagement>();

	public GeoWaveTransactionManagementState(
			final int transactionBufferSize,
			final GeoWaveDataStoreComponents components,
			final Transaction transaction,
			final LockingManagement lockingManager )
			throws IOException {
		this.transactionBufferSize = transactionBufferSize;
		this.components = components;
		this.transaction = transaction;
		this.lockingManager = lockingManager;
		txID = components.getTransaction();
	}

	@Override
	public synchronized void setTransaction(
			final Transaction transaction ) {
		if (transaction != null) {
			// configure
			this.transaction = transaction;
		}
		else {
			this.transaction = null;

			if (typeNameDiff != null) {
				for (final Iterator<GeoWaveTransactionManagement> i = typeNameDiff.values().iterator(); i.hasNext();) {
					final GeoWaveTransactionManagement diff = i.next();
					diff.clear();
				}

				typeNameDiff.clear();
			}
		}
	}

	@Override
	public synchronized GeoWaveTransactionManagement getGeoWaveTransaction(
			final String typeName )
			throws IOException {
		if (!exists(typeName)) {
			throw new RuntimeException(
					typeName + " not defined");
		}

		if (typeNameDiff.containsKey(typeName)) {
			return typeNameDiff.get(typeName);
		}
		else {
			final GeoWaveTransactionManagement transX = new GeoWaveTransactionManagement(
					transactionBufferSize,
					components,
					typeName,
					transaction,
					lockingManager,
					txID);
			typeNameDiff.put(
					typeName,
					transX);

			return transX;
		}
	}

	boolean exists(
			final String typeName )
			throws IOException {
		String[] types;
		types = components.getGTstore().getTypeNames();
		Arrays.sort(types);

		return Arrays.binarySearch(
				types,
				typeName) != -1;
	}

	/**
	 * @see org.geotools.data.Transaction.State#addAuthorization(java.lang.String)
	 */
	@Override
	public synchronized void addAuthorization(
			final String AuthID )
			throws IOException {
		// not required
	}

	/**
	 * Will apply differences to store.
	 * 
	 * @see org.geotools.data.Transaction.State#commit()
	 */
	@Override
	public synchronized void commit()
			throws IOException {

		try {
			for (final Iterator<Entry<String, GeoWaveTransactionManagement>> i = typeNameDiff.entrySet().iterator(); i
					.hasNext();) {
				final Map.Entry<String, GeoWaveTransactionManagement> entry = i.next();

				final String typeName = entry.getKey();
				final GeoWaveTransactionManagement diff = entry.getValue();
				applyDiff(
						typeName,
						diff);
				diff.clear();
			}
		}
		finally {
			components.releaseTransaction(txID);
		}
	}

	/**
	 * Called by commit() to apply one set of diff
	 * 
	 * <p>
	 * The provided <code> will be modified as the differences are applied,
	 * If the operations are all successful diff will be empty at
	 * the end of this process.
	 * </p>
	 * 
	 * <p>
	 * diff can be used to represent the following operations:
	 * </p>
	 * 
	 * <ul>
	 * <li>
	 * fid|null: represents a fid being removed</li>
	 * 
	 * <li>
	 * fid|feature: where fid exists, represents feature modification</li>
	 * <li>
	 * fid|feature: where fid does not exist, represents feature being modified</li>
	 * </ul>
	 * 
	 * 
	 * @param typeName
	 *            typeName being updated
	 * @param diff
	 *            differences to apply to FeatureWriter
	 * 
	 * @throws IOException
	 *             If the entire diff cannot be writen out
	 * @throws DataSourceException
	 *             If the entire diff cannot be writen out
	 */
	void applyDiff(
			final String typeName,
			final GeoWaveTransactionManagement diff )
			throws IOException {
		IOException cause = null;
		if (diff.isEmpty()) {
			return;
		}
		try {
			diff.commit();
		}
		catch (final IOException e) {
			cause = e;
			throw e;
		}
		catch (final RuntimeException e) {
			cause = new IOException(
					e);
			throw e;
		}
		finally {
			try {
				components.getGTstore().getListenerManager().fireChanged(
						typeName,
						transaction,
						true);
				diff.clear();
			}
			catch (final RuntimeException e) {
				if (cause != null) {
					e.initCause(cause);
				}
				throw e;
			}
		}
	}

	/**
	 * @see org.geotools.data.Transaction.State#rollback()
	 */
	@Override
	public synchronized void rollback()
			throws IOException {
		Entry<String, GeoWaveTransactionManagement> entry;

		try {
			for (final Iterator<Entry<String, GeoWaveTransactionManagement>> i = typeNameDiff.entrySet().iterator(); i
					.hasNext();) {
				entry = i.next();

				final String typeName = entry.getKey();
				final GeoWaveTransactionManagement diff = entry.getValue();
				diff.rollback();

				components.getGTstore().getListenerManager().fireChanged(
						typeName,
						transaction,
						false);
			}
		}
		finally {
			components.releaseTransaction(txID);
		}
	}

	@Override
	public String toString() {
		return "GeoWaveTransactionManagementState [components=" + components + ", lockingManager=" + lockingManager
				+ ", transaction=" + transaction + ", txID=" + txID + ", typeNameDiff=" + typeNameDiff + "]";
	}

}
