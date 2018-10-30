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
package org.locationtech.geowave.datastore.accumulo.split;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.cli.ParseException;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloRequiredOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractAccumuloSplitsOperation
{
	private static Logger LOGGER = LoggerFactory.getLogger(AbstractAccumuloSplitsOperation.class);

	private final DataStorePluginOptions storeOptions;
	private final SplitCommandLineOptions splitOptions;

	public AbstractAccumuloSplitsOperation(
			final DataStorePluginOptions storeOptions,
			final SplitCommandLineOptions splitOptions ) {
		this.storeOptions = storeOptions;
		this.splitOptions = splitOptions;
	}

	public boolean runOperation()
			throws ParseException {

		try {
			final IndexStore indexStore = storeOptions.createIndexStore();

			final AccumuloRequiredOptions options = (AccumuloRequiredOptions) storeOptions.getFactoryOptions();
			final AccumuloOperations operations = AccumuloOperations.createOperations(options);

			final Connector connector = operations.getConnector();
			final String namespace = options.getGeowaveNamespace();
			final long number = splitOptions.getNumber();
			if (splitOptions.getIndexName() == null) {
				boolean retVal = false;
				try (CloseableIterator<Index> indices = indexStore.getIndices()) {
					if (indices.hasNext()) {
						retVal = true;
					}
					while (indices.hasNext()) {
						final Index index = indices.next();
						if (!setSplits(
								connector,
								index,
								namespace,
								number)) {
							retVal = false;
						}
					}
				}
				if (!retVal) {
					LOGGER.error("no indices were successfully split, try providing an indexId");
				}
				return retVal;
			}
			else if (isPreSplit()) {
				setSplits(
						connector,
						new NullIndex(
								splitOptions.getIndexName()),
						namespace,
						number);
			}
			else {
				final Index index = indexStore.getIndex(splitOptions.getIndexName());
				if (index == null) {
					LOGGER.error("index '" + splitOptions.getIndexName() + "' does not exist; unable to create splits");
				}
				return setSplits(
						connector,
						index,
						namespace,
						number);
			}
		}
		catch (final AccumuloSecurityException | AccumuloException e) {
			LOGGER.error(
					"unable to create index store",
					e);
			return false;
		}
		return true;
	}

	protected boolean isPreSplit() {
		return false;
	};

	abstract protected boolean setSplits(
			Connector connector,
			Index index,
			String namespace,
			long number );
}
