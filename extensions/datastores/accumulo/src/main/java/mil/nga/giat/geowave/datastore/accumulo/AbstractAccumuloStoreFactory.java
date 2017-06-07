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
package mil.nga.giat.geowave.datastore.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;

abstract public class AbstractAccumuloStoreFactory<T> extends
		AbstractAccumuloFactory implements
		GenericStoreFactory<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractAccumuloStoreFactory.class);

	protected BasicAccumuloOperations createOperations(
			AccumuloRequiredOptions options ) {
		try {
			return BasicAccumuloOperations.createOperations(options);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to create Accumulo operations from config options",
					e);
			return null;
		}
	}

}
