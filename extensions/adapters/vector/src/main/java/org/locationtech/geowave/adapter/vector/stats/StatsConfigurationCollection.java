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
package org.locationtech.geowave.adapter.vector.stats;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.locationtech.geowave.core.geotime.util.SimpleFeatureUserDataConfiguration;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of statistics configurations targeted to a specific attribute.
 * Each configuration describes how to construct a statistic for an attribute.
 *
 */
public class StatsConfigurationCollection implements
		java.io.Serializable,
		Persistable
{

	private static final long serialVersionUID = -4983543525776889248L;

	private final static Logger LOGGER = LoggerFactory.getLogger(StatsConfigurationCollection.class);

	private List<StatsConfig<SimpleFeature>> configurationsForAttribute;

	public StatsConfigurationCollection() {

	}

	public StatsConfigurationCollection(
			final List<StatsConfig<SimpleFeature>> configurationsForAttribute ) {
		this.configurationsForAttribute = configurationsForAttribute;
	}

	public List<StatsConfig<SimpleFeature>> getConfigurationsForAttribute() {
		return configurationsForAttribute;
	}

	public void setConfigurationsForAttribute(
			final List<StatsConfig<SimpleFeature>> configrationsForAttribute ) {
		configurationsForAttribute = configrationsForAttribute;
	}

	@Override
	public byte[] toBinary() {
		return PersistenceUtils.toBinary(configurationsForAttribute);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		configurationsForAttribute = (List) PersistenceUtils.fromBinaryAsList(bytes);
	}

	public static class SimpleFeatureStatsConfigurationCollection implements
			SimpleFeatureUserDataConfiguration
	{

		private static final long serialVersionUID = -9149753182284018327L;
		private Map<String, StatsConfigurationCollection> attConfig = new HashMap<>();

		public SimpleFeatureStatsConfigurationCollection() {}

		public SimpleFeatureStatsConfigurationCollection(
				final SimpleFeatureType type ) {
			super();
			configureFromType(type);
		}

		public Map<String, StatsConfigurationCollection> getAttConfig() {
			return attConfig;
		}

		public void setAttConfig(
				final Map<String, StatsConfigurationCollection> attConfig ) {
			this.attConfig = attConfig;
		}

		@Override
		public void updateType(
				final SimpleFeatureType type ) {
			for (final Map.Entry<String, StatsConfigurationCollection> item : attConfig.entrySet()) {
				final AttributeDescriptor desc = type.getDescriptor(item.getKey());
				if (desc == null) {
					LOGGER.error("Attribute " + item.getKey() + " not found for statistics configuration");
					continue;
				}
				desc.getUserData().put(
						"stats",
						item.getValue());
			}
		}

		@Override
		public void configureFromType(
				final SimpleFeatureType type ) {
			for (final AttributeDescriptor descriptor : type.getAttributeDescriptors()) {
				if (descriptor.getUserData().containsKey(
						"stats")) {
					final Object configObj = descriptor.getUserData().get(
							"stats");
					if (!(configObj instanceof StatsConfigurationCollection)) {
						LOGGER.error("Invalid entry stats entry for " + descriptor.getLocalName());
						continue;
					}
					attConfig.put(
							descriptor.getLocalName(),
							(StatsConfigurationCollection) configObj);
				}
			}
		}

		@Override
		public byte[] toBinary() {
			int size = 4;
			final List<byte[]> entries = new ArrayList<>(
					attConfig.size());
			for (final Entry<String, StatsConfigurationCollection> e : attConfig.entrySet()) {
				final byte[] keyBytes = StringUtils.stringToBinary(e.getKey());
				int entrySize = 8 + keyBytes.length;
				final byte[] confBytes = PersistenceUtils.toBinary(e.getValue());
				entrySize += confBytes.length;
				size += entrySize;
				final ByteBuffer buf = ByteBuffer.allocate(entrySize);
				buf.putInt(keyBytes.length);
				buf.put(keyBytes);
				buf.putInt(confBytes.length);
				buf.put(confBytes);
				entries.add(buf.array());
			}
			final ByteBuffer buf = ByteBuffer.allocate(size);
			buf.putInt(attConfig.size());
			for (final byte[] e : entries) {
				buf.put(e);
			}
			return buf.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			final int entrySize = buf.getInt();
			final Map<String, StatsConfigurationCollection> internalAttConfig = new HashMap<>(
					entrySize);
			for (int i = 0; i < entrySize; i++) {
				final int keySize = buf.getInt();
				final byte[] keyBytes = new byte[keySize];
				buf.get(keyBytes);
				final String key = StringUtils.stringFromBinary(keyBytes);
				final byte[] entryBytes = new byte[buf.getInt()];
				buf.get(entryBytes);

				internalAttConfig.put(
						key,
						(StatsConfigurationCollection) PersistenceUtils.fromBinary(entryBytes));
			}
			attConfig = internalAttConfig;
		}
	}
}
