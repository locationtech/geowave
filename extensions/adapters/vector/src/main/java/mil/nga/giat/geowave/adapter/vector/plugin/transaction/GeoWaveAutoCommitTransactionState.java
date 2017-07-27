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

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveFeatureSource;

import org.geotools.data.Transaction;

public class GeoWaveAutoCommitTransactionState implements
		GeoWaveTransactionState
{

	private final GeoWaveDataStoreComponents components;

	public GeoWaveAutoCommitTransactionState(
			GeoWaveFeatureSource source ) {
		this.components = source.getComponents();
	}

	@Override
	public void setTransaction(
			Transaction transaction ) {}

	/**
	 * @see org.geotools.data.Transaction.State#addAuthorization(java.lang.String)
	 */
	@Override
	public void addAuthorization(
			String AuthID )
			throws IOException {
		// not required for
	}

	/**
	 * Will apply differences to store.
	 * 
	 * @see org.geotools.data.Transaction.State#commit()
	 */
	@Override
	public void commit()
			throws IOException {
		// not required for
	}

	/**
	 * @see org.geotools.data.Transaction.State#rollback()
	 */
	@Override
	public void rollback()
			throws IOException {

	}

	@Override
	public GeoWaveTransaction getGeoWaveTransaction(
			String typeName ) {
		// TODO Auto-generated method stub
		return new GeoWaveEmptyTransaction(
				components);
	}

	@Override
	public String toString() {
		return "GeoWaveAutoCommitTransactionState";
	}
}
