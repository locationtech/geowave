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
package org.locationtech.geowave.datastore.accumulo.operations;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;

/**
 * This is a basic wrapper around the Accumulo batch writer so that write
 * operations will use an interface that can be implemented differently for
 * different purposes. For example, a bulk ingest can be performed by replacing
 * this implementation within a custom implementation of AccumuloOperations.
 */
public class AccumuloWriter implements
		RowWriter
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloWriter.class);
	private org.apache.accumulo.core.client.BatchWriter batchWriter;
	private final AccumuloOperations operations;
	private final String tableName;

	public AccumuloWriter(
			final org.apache.accumulo.core.client.BatchWriter batchWriter,
			final AccumuloOperations operations,
			final String tableName ) {
		this.batchWriter = batchWriter;
		this.operations = operations;
		this.tableName = tableName;
	}

	public org.apache.accumulo.core.client.BatchWriter getBatchWriter() {
		return batchWriter;
	}

	public void setBatchWriter(
			final org.apache.accumulo.core.client.BatchWriter batchWriter ) {
		this.batchWriter = batchWriter;
	}

	public void write(
			final Iterable<Mutation> mutations ) {
		try {
			batchWriter.addMutations(mutations);
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to close batch writer",
					e);
		}
	}

	public void write(
			final Mutation mutation ) {
		try {
			batchWriter.addMutation(mutation);
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to write batch writer",
					e);
		}
	}

	@Override
	public void close() {
		try {
			batchWriter.close();
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to close batch writer",
					e);
		}
	}

	@Override
	public void flush() {
		try {
			batchWriter.flush();
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to flush batch writer",
					e);
		}
	}

	@Override
	public void write(
			final GeoWaveRow[] rows ) {
		for (final GeoWaveRow row : rows) {
			write(row);
		}
	}

	@Override
	public void write(
			final GeoWaveRow row ) {
		final byte[] partition = row.getPartitionKey();
		if ((partition != null) && (partition.length > 0)) {
			operations.ensurePartition(
					new ByteArray(
							partition),
					tableName);
		}
		write(rowToMutation(row));
	}

	public static Mutation rowToMutation(
			final GeoWaveRow row ) {
		final Mutation mutation = new Mutation(
				GeoWaveKey.getCompositeId(row));
		for (final GeoWaveValue value : row.getFieldValues()) {
			if ((value.getVisibility() != null) && (value.getVisibility().length > 0)) {
				mutation.put(
						new Text(
								ByteArrayUtils.shortToString(row.getAdapterId())),
						new Text(
								value.getFieldMask()),
						new ColumnVisibility(
								value.getVisibility()),
						new Value(
								value.getValue()));
			}
			else {
				mutation.put(
						new Text(
								ByteArrayUtils.shortToString(row.getAdapterId())),
						new Text(
								value.getFieldMask()),
						new Value(
								value.getValue()));
			}
		}
		return mutation;
	}
}
