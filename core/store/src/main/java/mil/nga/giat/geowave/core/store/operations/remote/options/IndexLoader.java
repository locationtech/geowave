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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

/**
 * This is a convenience class to load the desired indexes from either
 * IndexPluginOptions or IndexGroupPluginOptions based on the name of the index
 * (not the type). It will load from config file.
 */
public class IndexLoader
{

	private final String indexName;

	private Map<String, IndexPluginOptions> loadedIndices;

	/**
	 * Constructor
	 */
	public IndexLoader(
			final String indexName ) {
		this.indexName = indexName;
	}

	/**
	 * Attempt to find an index group or index name in the config file with the
	 * given name.
	 *
	 * @param configFile
	 * @return
	 */
	public boolean loadFromConfig(
			final File configFile ) {

		loadedIndices = new HashMap<String, IndexPluginOptions>();

		// Properties (load them all)
		return loadFromConfig(ConfigOptions.loadProperties(configFile));
	}

	/**
	 * Attempt to find an index group or index name in the config file with the
	 * given name.
	 *
	 * @param configFile
	 * @return
	 */
	public boolean loadFromConfig(
			final Properties props ) {

		loadedIndices = new HashMap<String, IndexPluginOptions>();

		// Is there a comma?
		final String[] indices = indexName.split(",");
		for (final String index : indices) {

			// Attempt to load as an index group first.
			final IndexGroupPluginOptions indexGroupOptions = loadIndexGroupPluginOptions(
					props,
					index);

			// Attempt to load as an index next.
			final IndexPluginOptions indexOptions = loadIndexPluginOptions(
					props,
					index);

			if ((indexGroupOptions != null) && (indexOptions != null)) {
				throw new ParameterException(
						"Aborting because there is both an index group " + "and index with the name: " + indexName);
			}
			else if (indexOptions != null) {
				loadedIndices.put(
						index,
						indexOptions);
			}
			else if (indexGroupOptions != null) {
				loadedIndices.putAll(indexGroupOptions.getDimensionalityPlugins());
			}
		}

		return loadedIndices.size() != 0;
	}

	private static IndexGroupPluginOptions loadIndexGroupPluginOptions(
			final Properties props,
			final String name ) {
		final IndexGroupPluginOptions indexGroupPlugin = new IndexGroupPluginOptions();
		final String indexGroupNamespace = IndexGroupPluginOptions.getIndexGroupNamespace(name);
		if (!indexGroupPlugin.load(
				props,
				indexGroupNamespace)) {
			return null;
		}
		return indexGroupPlugin;
	}

	private static IndexPluginOptions loadIndexPluginOptions(
			final Properties props,
			final String name ) {
		final IndexPluginOptions indexPlugin = new IndexPluginOptions();
		final String indexNamespace = IndexPluginOptions.getIndexNamespace(name);
		if (!indexPlugin.load(
				props,
				indexNamespace)) {
			return null;
		}
		return indexPlugin;
	}

	public List<IndexPluginOptions> getLoadedIndexes() {
		return Collections.unmodifiableList(new ArrayList<IndexPluginOptions>(
				loadedIndices.values()));
	}

	public void addIndex(
			final String indexName,
			final IndexPluginOptions option ) {
		if (loadedIndices == null) {
			loadedIndices = new HashMap<String, IndexPluginOptions>();
		}
		loadedIndices.put(
				indexName,
				option);
	}

	public void setLoadedIndices(
			final Map<String, IndexPluginOptions> loadedIndexes ) {
		loadedIndices = loadedIndexes;
	}

	public String getIndexName() {
		return indexName;
	}

}
