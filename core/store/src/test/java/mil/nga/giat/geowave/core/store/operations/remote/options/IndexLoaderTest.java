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
package mil.nga.giat.geowave.core.store.operations.remote.options;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.operations.config.AddIndexCommand;
import mil.nga.giat.geowave.core.store.operations.config.AddIndexGroupCommand;

public class IndexLoaderTest
{
	@Test
	public void testMultipleIndices()
			throws IOException {
		final AddIndexCommand addIndex = new AddIndexCommand();
		addIndex.setType("test");
		addIndex.setParameters("index1");
		final ManualOperationParams params = new ManualOperationParams();
		final File props = File.createTempFile(
				"IndexLoaderTest.testMultipleIndices",
				"props");
		props.deleteOnExit();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				props);
		addIndex.prepare(params);
		addIndex.execute(params);
		addIndex.setParameters("index2");
		addIndex.prepare(params);
		addIndex.execute(params);
		final IndexLoader loader = new IndexLoader(
				"index1,index2");
		Assert.assertTrue(
				"Unable to load multiple indices from property file",
				loader.loadFromConfig(props));
		final List<IndexPluginOptions> options = loader.getLoadedIndexes();
		Assert.assertEquals(
				"Given multiple indices an incorrect number of indices loaded",
				2,
				options.size());
	}

	@Test
	public void testIndexGroup()
			throws IOException {
		final AddIndexCommand addIndex = new AddIndexCommand();
		addIndex.setType("test");
		addIndex.setParameters("index1");
		final ManualOperationParams params = new ManualOperationParams();
		final File props = File.createTempFile(
				"IndexLoaderTest.testIndexGroup",
				"props");
		props.deleteOnExit();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				props);
		addIndex.prepare(params);
		addIndex.execute(params);
		addIndex.setParameters("index2");
		addIndex.prepare(params);
		addIndex.execute(params);
		final AddIndexGroupCommand addIndexGroup = new AddIndexGroupCommand();
		addIndexGroup.setParameters(
				"indexGroup1",
				"index1,index2");
		addIndexGroup.prepare(params);
		addIndexGroup.execute(params);
		final IndexLoader loader = new IndexLoader(
				"indexGroup1");
		Assert.assertTrue(
				"Unable to load index groups from property file",
				loader.loadFromConfig(props));
		final List<IndexPluginOptions> options = loader.getLoadedIndexes();
		Assert.assertEquals(
				"Given a single index group an incorrect number of indices loaded",
				2,
				options.size());
	}

	@Test
	public void testMutlipleIndexGroupsAndIndices()
			throws IOException {
		final AddIndexCommand addIndex = new AddIndexCommand();
		addIndex.setType("test");
		addIndex.setParameters("index1");
		final ManualOperationParams params = new ManualOperationParams();
		final File props = File.createTempFile(
				"IndexLoaderTest.testMutlipleIndexGroupsAndIndices",
				"props");
		props.deleteOnExit();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				props);
		addIndex.prepare(params);
		addIndex.execute(params);
		addIndex.setParameters("index2");
		addIndex.prepare(params);
		addIndex.execute(params);
		addIndex.setParameters("index3");
		addIndex.prepare(params);
		addIndex.execute(params);
		addIndex.setParameters("index4");
		addIndex.prepare(params);
		addIndex.execute(params);
		final AddIndexGroupCommand addIndexGroup = new AddIndexGroupCommand();
		addIndexGroup.setParameters(
				"indexGroup1",
				"index1,index2");
		addIndexGroup.prepare(params);
		addIndexGroup.execute(params);
		addIndexGroup.setParameters(
				"indexGroup2",
				"index3,index2");
		addIndexGroup.prepare(params);
		addIndexGroup.execute(params);
		final IndexLoader loader = new IndexLoader(
				"indexGroup1,indexGroup2,index4,index2");
		Assert.assertTrue(
				"Unable to load combination of index groups and indices from property file",
				loader.loadFromConfig(props));
		final List<IndexPluginOptions> options = loader.getLoadedIndexes();
		Assert.assertEquals(
				"Given the combination of indices and index groups an incorrect number of indices loaded",
				4,
				options.size());
	}

}
