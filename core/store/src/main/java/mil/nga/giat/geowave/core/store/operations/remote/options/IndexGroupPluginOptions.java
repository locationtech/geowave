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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.api.DefaultPluginOptions;
import mil.nga.giat.geowave.core.cli.api.PluginOptions;

/**
 * Index group contains a list of indexes that are created from existing index
 * configurations.
 */
public class IndexGroupPluginOptions extends
		DefaultPluginOptions implements
		PluginOptions
{

	public final static String INDEXGROUP_PROPERTY_NAMESPACE = "indexgroup";

	/**
	 * This is indexed by index name, instead of what you'd expect, which would
	 * be index type.
	 */
	@ParametersDelegate
	private Map<String, IndexPluginOptions> dimensionalityPlugins = new HashMap<String, IndexPluginOptions>();

	public IndexGroupPluginOptions() {

	}

	/**
	 * The name of this index group.
	 */
	@Override
	public void selectPlugin(
			String qualifier ) {
		// This is specified as so: name=type,name=type,...
		if (qualifier != null && qualifier.length() > 0) {
			for (String name : qualifier.split(",")) {
				String[] parts = name.split("=");
				addIndex(
						parts[0].trim(),
						parts[1].trim());
			}
		}
	}

	@Override
	public String getType() {
		List<String> typeString = new ArrayList<String>();
		for (Entry<String, IndexPluginOptions> entry : getDimensionalityPlugins().entrySet()) {
			typeString.add(String.format(
					"%s=%s",
					entry.getKey(),
					entry.getValue().getType()));
		}
		if (typeString.isEmpty()) {
			return null;
		}
		return StringUtils.join(
				typeString,
				",");
	}

	public void addIndex(
			String name,
			String type ) {
		if (name != null && type != null) {
			IndexPluginOptions indexOptions = new IndexPluginOptions();
			indexOptions.selectPlugin(type);
			getDimensionalityPlugins().put(
					name,
					indexOptions);
		}
	}

	public Map<String, IndexPluginOptions> getDimensionalityPlugins() {
		return dimensionalityPlugins;
	}

	public static String getIndexGroupNamespace(
			String groupName ) {
		return String.format(
				"%s.%s",
				INDEXGROUP_PROPERTY_NAMESPACE,
				groupName);
	}

}
