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

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloRowDeleter implements
		RowDeleter
{
	private static Logger LOGGER = LoggerFactory.getLogger(AccumuloRowDeleter.class);
	private final BatchDeleter deleter;

	public AccumuloRowDeleter(
			final BatchDeleter deleter ) {
		this.deleter = deleter;
	}

	@Override
	public void close() {
		deleter.close();
	}

	public BatchDeleter getDeleter() {
		return deleter;
	}

	@Override
	public synchronized void delete(
			final GeoWaveRow row ) {
		final List<Range> rowRanges = new ArrayList<Range>();
		rowRanges.add(Range.exact(new Text(
				GeoWaveKey.getCompositeId(row))));
		final BatchDeleter batchDeleter = getDeleter();
		batchDeleter.setRanges(rowRanges);
		try {
			batchDeleter.delete();
		}
		catch (MutationsRejectedException | TableNotFoundException e) {
			LOGGER.warn(
					"Unable to delete row: " + row.toString(),
					e);
		}
	}
}
