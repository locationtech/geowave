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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;

public class HBaseMetadataDeleter implements
		MetadataDeleter
{
	private final static Logger LOGGER = Logger.getLogger(HBaseMetadataDeleter.class);

	private final HBaseOperations operations;
	private final MetadataType metadataType;

	public HBaseMetadataDeleter(
			final HBaseOperations operations,
			final MetadataType metadataType ) {
		super();
		this.operations = operations;
		this.metadataType = metadataType;
	}

	@Override
	public void close()
			throws Exception {}

	@Override
	public boolean delete(
			final MetadataQuery query ) {
		// the nature of metadata deleter is that primary ID is always
		// well-defined and it is deleting a single entry at a time
		TableName tableName = operations.getTableName(operations.getMetadataTableName(metadataType));

		try {
			BufferedMutator deleter = operations.getBufferedMutator(tableName);

			Delete delete = new Delete(
					query.getPrimaryId());
			delete.addColumns(
					StringUtils.stringToBinary(metadataType.name()),
					query.getSecondaryId());

			deleter.mutate(delete);
			deleter.close();

			return true;
		}
		catch (IOException e) {
			LOGGER.error(
					"Error deleting metadata",
					e);
		}

		return false;
	}

	@Override
	public void flush() {}

}
