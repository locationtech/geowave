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
package mil.nga.giat.geowave.adapter.vector.stats;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

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
		this.configurationsForAttribute = configrationsForAttribute;
	}

	@Override
	public byte[] toBinary() {
		return PersistenceUtils.toBinary(configurationsForAttribute);
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		configurationsForAttribute = (List) PersistenceUtils.fromBinaryAsList(bytes);
	}

	public static class SimpleFeatureStatsConfigurationCollection implements
			SimpleFeatureUserDataConfiguration
	{

		private static final long serialVersionUID = -9149753182284018327L;
		private Map<String, StatsConfigurationCollection> attConfig = new HashMap<String, StatsConfigurationCollection>();

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
			List<byte[]> entries = new ArrayList<>(
					attConfig.size());
			for (Entry<String, StatsConfigurationCollection> e : attConfig.entrySet()) {
				byte[] keyBytes = StringUtils.stringToBinary(e.getKey());
				int entrySize = 8 + keyBytes.length;
				byte[] confBytes = PersistenceUtils.toBinary(e.getValue());
				entrySize += confBytes.length;
				size += entrySize;
				ByteBuffer buf = ByteBuffer.allocate(entrySize);
				buf.putInt(keyBytes.length);
				buf.put(keyBytes);
				buf.putInt(confBytes.length);
				buf.put(confBytes);
				entries.add(buf.array());
			}
			ByteBuffer buf = ByteBuffer.allocate(size);
			buf.putInt(attConfig.size());
			for (byte[] e : entries) {
				buf.put(e);
			}
			return buf.array();
		}

		@Override
		public void fromBinary(
				byte[] bytes ) {
			ByteBuffer buf = ByteBuffer.wrap(bytes);
			int entrySize = buf.getInt();
			Map<String, StatsConfigurationCollection> internalAttConfig = new HashMap<>(
					entrySize);
			for (int i = 0; i < entrySize; i++) {
				int keySize = buf.getInt();
				byte[] keyBytes = new byte[keySize];
				buf.get(keyBytes);
				String key = StringUtils.stringFromBinary(keyBytes);
				byte[] entryBytes = new byte[buf.getInt()];
				buf.get(entryBytes);

				internalAttConfig.put(
						key,
						(StatsConfigurationCollection) PersistenceUtils.fromBinary(entryBytes));
			}
			this.attConfig = internalAttConfig;
		}
	}
}
