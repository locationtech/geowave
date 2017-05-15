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
package mil.nga.giat.geowave.datastore.hbase;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseRequiredOptions;

abstract public class AbstractHBaseStoreFactory<T> extends
		AbstractHBaseFactory implements
		GenericStoreFactory<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractHBaseStoreFactory.class);

	protected BasicHBaseOperations createOperations(
			final HBaseRequiredOptions options ) {
		try {
			return BasicHBaseOperations.createOperations(options);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to create HBase operations from config options",
					e);
			return null;
		}
	}
}
