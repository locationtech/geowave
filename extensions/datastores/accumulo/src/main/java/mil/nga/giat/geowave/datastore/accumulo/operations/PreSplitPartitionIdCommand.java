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
package mil.nga.giat.geowave.datastore.accumulo.operations;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.split.AbstractAccumuloSplitsOperation;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

@GeowaveOperation(name = "presplitpartitionid", parentOperation = AccumuloSection.class)
@Parameters(commandDescription = "Pre-split Accumulo table by providing the number of partition IDs")
public class PreSplitPartitionIdCommand extends
		AbstractSplitsCommand implements
		Command
{

	private final static Logger LOGGER = LoggerFactory.getLogger(PreSplitPartitionIdCommand.class);

	@Override
	public void doSplit()
			throws Exception {

		new AbstractAccumuloSplitsOperation(
				inputStoreOptions,
				splitOptions) {

			@Override
			protected boolean setSplits(
					final Connector connector,
					final PrimaryIndex index,
					final String namespace,
					final long number ) {
				try {
					AccumuloUtils.setSplitsByRandomPartitions(
							connector,
							namespace,
							index,
							(int) number);
				}
				catch (AccumuloException | AccumuloSecurityException | IOException | TableNotFoundException e) {
					LOGGER.error(
							"Error pre-splitting",
							e);
					return false;
				}
				return true;
			}

			@Override
			protected boolean isPreSplit() {
				return true;
			}

		}.runOperation();

	}
}
