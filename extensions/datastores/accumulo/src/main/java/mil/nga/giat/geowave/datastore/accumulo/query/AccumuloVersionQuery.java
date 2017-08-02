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
package mil.nga.giat.geowave.datastore.accumulo.query;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeowavePersistence;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;

public class AccumuloVersionQuery
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloVersionQuery.class);
	private final AccumuloOperations accumuloOperations;

	public AccumuloVersionQuery(
			final AccumuloOperations accumuloOperations ) {
		this.accumuloOperations = accumuloOperations;
	}

	public String queryVersion() {
		// this just creates it if it doesn't exist
		accumuloOperations.createTable(
				AbstractGeowavePersistence.METADATA_TABLE,
				true,
				true,
				null);
		try {
			final Scanner scanner = accumuloOperations.createScanner(AbstractGeowavePersistence.METADATA_TABLE);
			scanner.addScanIterator(new IteratorSetting(
					25,
					VersionIterator.class));
			return StringUtils.stringFromBinary(scanner.iterator().next().getValue().get());
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable to get GeoWave version from Accumulo",
					e);
		}
		return null;
	}
}
