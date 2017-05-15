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
package mil.nga.giat.geowave.core.store.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPropertiesTransformer;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;
import mil.nga.giat.geowave.core.cli.prefix.TranslationEntry;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

public class ConfigUtils
{
	public static String cleanOptionName(
			String name ) {
		name = name.trim().toLowerCase(
				Locale.ENGLISH).replaceAll(
				" ",
				"_");
		name = name.replaceAll(
				",",
				"");
		return name;
	}

	public static StringBuilder getOptions(
			final Collection<String> strs,
			final String prefixStr ) {

		final StringBuilder builder = new StringBuilder();
		for (final String str : strs) {
			if (builder.length() > 0) {
				builder.append(",");
			}
			else {
				builder.append(prefixStr);
			}
			builder.append(
					"'").append(
					cleanOptionName(str)).append(
					"'");
		}
		return builder;
	}

	public static StringBuilder getOptions(
			final Collection<String> strs ) {
		return getOptions(
				strs,
				"Options include: ");
	}

	/**
	 * This method will use the parameter descriptions from JCommander to
	 * create/populate an AbstractConfigOptions map.
	 */
	public static ConfigOption[] createConfigOptionsFromJCommander(
			final Object createOptionsInstance,
			boolean includeHidden ) {
		ConfigOption[] opts = null;
		if (createOptionsInstance != null) {
			final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
			translator.addObject(createOptionsInstance);
			final JCommanderTranslationMap map = translator.translate();
			final Collection<TranslationEntry> entries = map.getEntries().values();
			final List<ConfigOption> options = new ArrayList<ConfigOption>();
			for (final TranslationEntry entry : entries) {
				if (includeHidden || !entry.isHidden()) {
					final ConfigOption opt = new ConfigOption(
							entry.getAsPropertyName(),
							entry.getDescription(),
							!entry.isRequired(),
							entry.getParam().getType());
					opt.setPassword(entry.isPassword());
					options.add(opt);
				}
			}
			opts = options.toArray(new ConfigOption[options.size()]);
		}
		else {
			opts = new ConfigOption[0];
		}
		return opts;
	}

	/**
	 * Take the given options and populate the given options list. This is
	 * JCommander specific.
	 */
	public static <T extends StoreFactoryOptions> T populateOptionsFromList(
			final T optionsObject,
			final Map<String, String> optionList ) {
		if (optionsObject != null) {
			final JCommanderPropertiesTransformer translator = new JCommanderPropertiesTransformer();
			translator.addObject(optionsObject);
			translator.transformFromMap(optionList);
		}
		return optionsObject;
	}

	/**
	 * Take the given options and populate the given options list. This is
	 * JCommander specific.
	 */
	public static Map<String, String> populateListFromOptions(
			final StoreFactoryOptions optionsObject ) {
		final Map<String, String> mapOptions = new HashMap<String, String>();
		if (optionsObject != null) {
			final JCommanderPropertiesTransformer translator = new JCommanderPropertiesTransformer();
			translator.addObject(optionsObject);
			translator.transformToMap(mapOptions);
			mapOptions.put(
					GeoWaveStoreFinder.STORE_HINT_KEY,
					optionsObject.getStoreFactory().getType());
		}
		return mapOptions;
	}
}
