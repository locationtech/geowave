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
package org.locationtech.geowave.datastore.cassandra.config;

import org.locationtech.geowave.core.store.BaseDataStoreOptions;

import com.beust.jcommander.Parameter;

public class CassandraOptions extends
		BaseDataStoreOptions
{
	@Parameter(names = "--batchWriteSize", description = "The number of inserts in a batch write.")
	private int batchWriteSize = 50;
	@Parameter(names = "--durableWrites", description = "Whether to write to commit log for durability, configured only on creation of new keyspace.", arity = 1)
	private boolean durableWrites = true;
	@Parameter(names = "--replicas", description = "The number of replicas to use when creating a new keyspace.")
	private int replicationFactor = 3;

	// TODO: it'd be nice to offer an option to organize the data where the
	// space filling curve cluster column precedes the adapter ID so you can
	// query across adapters efficiently (ie. get all the points regardless of
	// feature type)
	// within geoserver this is not a default use case, but this is analogous to
	// the option of setting locality groups in accumulo
	// TODO: this is technically a property of the table and should be persisted
	// as table metadata
	// @Parameter(names = "--comingleTypes", description = "Store different
	// types together to quickly query across different types (this will have
	// performance implications on querying for a single type when storing
	// comingling multiple types in the same table).")
	// private boolean comingleTypes = false;

	// public boolean isComingleTypes() {
	// return comingleTypes;
	// }
	//
	// public void setComingleTypes(
	// boolean comingleTypes ) {
	// this.comingleTypes = comingleTypes;
	// }

	public int getBatchWriteSize() {
		return batchWriteSize;
	}

	public void setBatchWriteSize(
			final int batchWriteSize ) {
		this.batchWriteSize = batchWriteSize;
	}

	public boolean isDurableWrites() {
		return durableWrites;
	}

	public void setDurableWrites(
			final boolean durableWrites ) {
		this.durableWrites = durableWrites;
	}

	public int getReplicationFactor() {
		return replicationFactor;
	}

	public void setReplicationFactor(
			final int replicationFactor ) {
		this.replicationFactor = replicationFactor;
	}

	@Override
	public boolean isServerSideLibraryEnabled() {
		return false;
	}
}
