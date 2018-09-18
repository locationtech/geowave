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
package org.locationtech.geowave.datastore.cassandra.util;

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.datastore.cassandra.CassandraRow.CassandraField;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

public class CassandraUtils
{

	public static BoundStatement[] bindInsertion(
			final PreparedStatement insertionStatement,
			final GeoWaveRow row ) {
		final BoundStatement[] retVal = new BoundStatement[row.getFieldValues().length];
		int i = 0;
		for (final GeoWaveValue value : row.getFieldValues()) {
			ByteBuffer nanoBuffer = ByteBuffer.allocate(8);
			nanoBuffer.putLong(
					0,
					Long.MAX_VALUE - System.nanoTime());
			retVal[i] = new BoundStatement(
					insertionStatement);
			retVal[i].set(
					CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName(),
					ByteBuffer.wrap(row.getPartitionKey()),
					ByteBuffer.class);
			retVal[i].set(
					CassandraField.GW_SORT_KEY.getBindMarkerName(),
					ByteBuffer.wrap(row.getSortKey()),
					ByteBuffer.class);
			retVal[i].set(
					CassandraField.GW_DATA_ID_KEY.getBindMarkerName(),
					ByteBuffer.wrap(row.getDataId()),
					ByteBuffer.class);
			retVal[i].set(
					CassandraField.GW_FIELD_VISIBILITY_KEY.getBindMarkerName(),
					ByteBuffer.wrap(value.getVisibility()),
					ByteBuffer.class);
			retVal[i].set(
					CassandraField.GW_NANO_TIME_KEY.getBindMarkerName(),
					nanoBuffer,
					ByteBuffer.class);
			retVal[i].set(
					CassandraField.GW_FIELD_MASK_KEY.getBindMarkerName(),
					ByteBuffer.wrap(value.getFieldMask()),
					ByteBuffer.class);
			retVal[i].set(
					CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName(),
					row.getAdapterId(),
					Short.class);
			retVal[i].set(
					CassandraField.GW_VALUE_KEY.getBindMarkerName(),
					ByteBuffer.wrap(value.getValue()),
					ByteBuffer.class);
			retVal[i].set(
					CassandraField.GW_NUM_DUPLICATES_KEY.getBindMarkerName(),
					(byte) row.getNumberOfDuplicates(),
					byte.class);
			// ByteBuffer.wrap(new byte[] {
			// (byte) row.getNumberOfDuplicates()
			i++;
			// ByteBuffer.class);
		}
		return retVal;
	}

}
