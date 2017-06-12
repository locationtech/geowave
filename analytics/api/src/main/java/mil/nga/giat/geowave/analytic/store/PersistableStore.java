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
package mil.nga.giat.geowave.analytic.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

public class PersistableStore implements
		Persistable
{
	// Using this here instead of raw DataStorePluginOptions, so we can
	// use the convenient methods
	private DataStorePluginOptions pluginOptions;

	public PersistableStore() {}

	public PersistableStore(
			final DataStorePluginOptions options ) {
		pluginOptions = options;
	}

	public DataStorePluginOptions getDataStoreOptions() {
		return pluginOptions;
	}

	@Override
	public byte[] toBinary() {
		// Persist
		Properties strOptions = new Properties();
		pluginOptions.save(
				strOptions,
				null);
		final List<byte[]> strOptionsBinary = new ArrayList<byte[]>(
				strOptions.size());
		int optionsLength = 4;// for the size of the config options
		for (final String key : strOptions.stringPropertyNames()) {
			final byte[] keyBinary = StringUtils.stringToBinary(key);
			final byte[] valueBinary = StringUtils.stringToBinary(strOptions.getProperty(key));
			final int entryLength = keyBinary.length + valueBinary.length + 8;
			final ByteBuffer buf = ByteBuffer.allocate(entryLength);
			buf.putInt(keyBinary.length);
			buf.put(keyBinary);
			buf.putInt(valueBinary.length);
			buf.put(valueBinary);
			strOptionsBinary.add(buf.array());
			optionsLength += entryLength;
		}
		optionsLength += (8);
		final ByteBuffer buf = ByteBuffer.allocate(optionsLength);
		buf.putInt(strOptionsBinary.size());
		for (final byte[] strOption : strOptionsBinary) {
			buf.put(strOption);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int configOptionLength = buf.getInt();
		final Properties configOptions = new Properties();
		for (int i = 0; i < configOptionLength; i++) {
			final int keyLength = buf.getInt();
			final byte[] keyBinary = new byte[keyLength];
			buf.get(keyBinary);
			final int valueLength = buf.getInt();
			final byte[] valueBinary = new byte[valueLength];
			buf.get(valueBinary);
			configOptions.put(
					StringUtils.stringFromBinary(keyBinary),
					StringUtils.stringFromBinary(valueBinary));
		}
		pluginOptions = new DataStorePluginOptions();
		pluginOptions.load(
				configOptions,
				null);
	}
}
