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
package mil.nga.giat.geowave.datastore.accumulo.split;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.NullIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;

abstract public class AbstractAccumuloSplitsOperation
{
	private static Logger LOGGER = LoggerFactory.getLogger(AbstractAccumuloSplitsOperation.class);

	private final DataStorePluginOptions storeOptions;
	private final SplitCommandLineOptions splitOptions;

	public AbstractAccumuloSplitsOperation(
			DataStorePluginOptions storeOptions,
			SplitCommandLineOptions splitOptions ) {
		this.storeOptions = storeOptions;
		this.splitOptions = splitOptions;
	}

	public boolean runOperation()
			throws ParseException {

		try {
			final IndexStore indexStore = storeOptions.createIndexStore();

			AccumuloRequiredOptions options = (AccumuloRequiredOptions) storeOptions.getFactoryOptions();
			BasicAccumuloOperations operations = BasicAccumuloOperations.createOperations(options);

			final Connector connector = operations.getConnector();
			final String namespace = options.getGeowaveNamespace();
			final long number = splitOptions.getNumber();
			if (splitOptions.getIndexId() == null) {
				boolean retVal = false;
				try (CloseableIterator<Index<?, ?>> indices = indexStore.getIndices()) {
					if (indices.hasNext()) {
						retVal = true;
					}
					while (indices.hasNext()) {
						final Index index = indices.next();
						if (index instanceof PrimaryIndex) {
							if (!setSplits(
									connector,
									(PrimaryIndex) index,
									namespace,
									number)) {
								retVal = false;
							}
						}
					}
				}
				catch (final IOException e) {
					LOGGER.error(
							"unable to close index store",
							e);
					return false;
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
								splitOptions.getIndexId()),
						namespace,
						number);
			}
			else {
				final Index index = indexStore.getIndex(new ByteArrayId(
						splitOptions.getIndexId()));
				if (index == null) {
					LOGGER.error("index '" + splitOptions.getIndexId() + "' does not exist; unable to create splits");
				}
				if (!(index instanceof PrimaryIndex)) {
					LOGGER.error("index '" + splitOptions.getIndexId()
							+ "' is not a primary index; unable to create splits");
				}
				return setSplits(
						connector,
						(PrimaryIndex) index,
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
			PrimaryIndex index,
			String namespace,
			long number );
}
