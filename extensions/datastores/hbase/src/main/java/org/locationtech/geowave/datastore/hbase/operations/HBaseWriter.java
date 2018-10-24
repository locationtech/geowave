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
package org.locationtech.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;

/**
 * This is a basic wrapper around the HBase BufferedMutator so that write
 * operations will use an interface that can be implemented differently for
 * different purposes. For example, a bulk ingest can be performed by replacing
 * this implementation within a custom implementation of HBaseOperations.
 */
public class HBaseWriter implements
		RowWriter
{
	private final static Logger LOGGER = Logger.getLogger(HBaseWriter.class);

	protected Set<ByteArray> duplicateRowTracker = new HashSet<>();
	private final BufferedMutator mutator;

	public HBaseWriter(
			final BufferedMutator mutator ) {
		this.mutator = mutator;
	}

	@Override
	public void close() {
		try {
			mutator.close();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close BufferedMutator",
					e);
		}
	}

	@Override
	public void flush() {
		try {
			synchronized (duplicateRowTracker) {
				mutator.flush();
				duplicateRowTracker.clear();
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to flush BufferedMutator",
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
		writeMutations(rowToMutation(row));
	}

	private void writeMutations(
			final RowMutations rowMutation ) {
		try {
			mutator.mutate(rowMutation.getMutations());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to write mutation.",
					e);
		}
	}

	private RowMutations rowToMutation(
			final GeoWaveRow row ) {
		final byte[] rowBytes = GeoWaveKey.getCompositeId(row);

		// we use a hashset of row IDs so that we can retain multiple versions
		// (otherwise timestamps will be applied on the server side in
		// batches and if the same row exists within a batch we will not
		// retain multiple versions)
		synchronized (duplicateRowTracker) {
			final ByteArray rowId = new ByteArray(
					rowBytes);
			if (!duplicateRowTracker.add(rowId)) {
				try {
					mutator.flush();
					duplicateRowTracker.clear();
					duplicateRowTracker.add(rowId);
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to write mutation.",
							e);
				}
			}
		}

		final RowMutations mutation = new RowMutations(
				rowBytes);
		for (final GeoWaveValue value : row.getFieldValues()) {
			final Put put = new Put(
					rowBytes);

			put.addColumn(
					StringUtils.stringToBinary(ByteArrayUtils.shortToString(row.getAdapterId())),
					value.getFieldMask(),
					value.getValue());

			if ((value.getVisibility() != null) && (value.getVisibility().length > 0)) {
				put.setCellVisibility(new CellVisibility(
						StringUtils.stringFromBinary(value.getVisibility())));
			}

			try {
				mutation.add(put);
			}
			catch (final IOException e) {
				LOGGER.error("Error creating HBase row mutation: " + e.getMessage());
			}
		}

		return mutation;
	}
}
