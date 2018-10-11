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

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.Deleter;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.datastore.hbase.coprocessors.protobuf.HBaseBulkDeleteProtosClient;

public class HBaseDeleter<T> extends
		HBaseReader<T> implements
		Deleter<T>
{
	private boolean closed = false;
	private static final HBaseBulkDeleteProtosClient.BulkDeleteRequest.BulkDeleteType DELETE_TYPE = HBaseBulkDeleteProtosClient.BulkDeleteRequest.BulkDeleteType.ROW;

	public HBaseDeleter(
			final ReaderParams<T> readerParams,
			final HBaseOperations operations ) {
		super(
				readerParams,
				operations);
	}

	@Override
	public void close()
			throws Exception {
		if (!closed) {
			// make sure delete is only called once
			operations.bulkDelete(readerParams);

			closed = true;
		}
		super.close();
	}

	@Override
	public void entryScanned(
			T entry,
			GeoWaveRow row ) {

	}

}
