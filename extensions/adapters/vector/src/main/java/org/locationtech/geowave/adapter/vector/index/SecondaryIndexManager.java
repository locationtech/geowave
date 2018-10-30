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
package org.locationtech.geowave.adapter.vector.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.locationtech.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics;
import org.locationtech.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics;
import org.locationtech.geowave.adapter.vector.stats.StatsManager;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.index.SecondaryIndexImpl;
import org.locationtech.geowave.core.store.index.SecondaryIndexType;
import org.locationtech.geowave.core.store.index.numeric.NumericFieldIndexStrategy;
import org.locationtech.geowave.core.store.index.temporal.TemporalIndexStrategy;
import org.locationtech.geowave.core.store.index.text.TextIndexStrategy;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.google.common.base.Splitter;

/**
 * Class to manage secondary indexes for a Simple Feature Type. It keeps a list
 * of supported secondary indices associated with all the attributes attached to
 * the SimpleFeatureType provided upon instantiation.
 */

public class SecondaryIndexManager implements
		Persistable
{
	private final List<SecondaryIndexImpl<SimpleFeature>> supportedSecondaryIndices = new ArrayList<>();
	private transient SimpleFeatureType sft;
	private transient StatsManager statsManager;

	public SecondaryIndexManager() {}

	/**
	 * Create a SecondaryIndexManager for the given DataAdapter and
	 * SimpleFeatureType, while providing
	 *
	 * @param dataAdapter
	 * @param sft
	 * @param statsManager
	 */
	public SecondaryIndexManager(
			final SimpleFeatureType sft,
			final StatsManager statsManager ) {
		this.statsManager = statsManager;
		this.sft = sft;
		initializeIndices();
	}

	/**
	 * For every attribute of the SFT to be managed by this index, determine
	 * type and if found, create a secondaryIndex of strategy type Temporal,
	 * Text or Numeric for that attribute.
	 */
	private void initializeIndices() {
		for (final AttributeDescriptor desc : sft.getAttributeDescriptors()) {

			final Map<Object, Object> userData = desc.getUserData();
			final String attributeName = desc.getLocalName();
			String secondaryIndex = null;
			SecondaryIndexType secondaryIndexType = null;
			final List<String> fieldsForPartial = new ArrayList<>();

			if (userData.containsKey(NumericSecondaryIndexConfiguration.INDEX_KEY)) {
				secondaryIndex = NumericSecondaryIndexConfiguration.INDEX_KEY;
				secondaryIndexType = SecondaryIndexType.valueOf((String) userData
						.get(NumericSecondaryIndexConfiguration.INDEX_KEY));
			}
			else if (userData.containsKey(TextSecondaryIndexConfiguration.INDEX_KEY)) {
				secondaryIndex = TextSecondaryIndexConfiguration.INDEX_KEY;
				secondaryIndexType = SecondaryIndexType.valueOf((String) userData
						.get(TextSecondaryIndexConfiguration.INDEX_KEY));
			}
			else if (userData.containsKey(TemporalSecondaryIndexConfiguration.INDEX_KEY)) {
				secondaryIndex = TemporalSecondaryIndexConfiguration.INDEX_KEY;
				secondaryIndexType = SecondaryIndexType.valueOf((String) userData
						.get(TemporalSecondaryIndexConfiguration.INDEX_KEY));
			}

			// If a valid secondary index type is provided, and the type is
			// PARTIAL, then
			// go through list of fields to be joined at add to tracked list of
			// fieldsForPartial

			if (secondaryIndexType != null) {
				if (secondaryIndexType.equals(SecondaryIndexType.PARTIAL)) {
					final String joined = (String) userData.get(SecondaryIndexType.PARTIAL.getValue());
					final Iterable<String> split = Splitter.on(
							",").split(
							joined);
					for (final String field : split) {
						fieldsForPartial.add(field);
					}
				}
				addIndex(
						secondaryIndex,
						attributeName,
						secondaryIndexType,
						fieldsForPartial);
			}
		}
	}

	public List<SecondaryIndexImpl<SimpleFeature>> getSupportedSecondaryIndices() {
		return supportedSecondaryIndices;
	}

	/**
	 * Add an index-based secondary index key
	 *
	 * @param secondaryIndexKey
	 * @param fieldId
	 * @param secondaryIndexType
	 * @param fieldsForPartial
	 */

	private void addIndex(
			final String secondaryIndexKey,
			final String fieldName,
			final SecondaryIndexType secondaryIndexType,
			final List<String> fieldsForPartial ) {

		final List<InternalDataStatistics<SimpleFeature, ?, ?>> statistics = new ArrayList<>();
		InternalDataStatistics<SimpleFeature, ?, ?> stat = null;
		switch (secondaryIndexKey) {

			case NumericSecondaryIndexConfiguration.INDEX_KEY:
				stat = new FeatureNumericHistogramStatistics(
						fieldName);
				statistics.add(stat);
				supportedSecondaryIndices.add(new SecondaryIndexImpl<>(
						new NumericFieldIndexStrategy(),
						fieldName,
						statistics,
						secondaryIndexType,
						fieldsForPartial));
				break;

			case TextSecondaryIndexConfiguration.INDEX_KEY:
				stat = new FeatureHyperLogLogStatistics(
						fieldName,
						16);
				statistics.add(stat);
				supportedSecondaryIndices.add(new SecondaryIndexImpl<>(
						new TextIndexStrategy(),
						fieldName,
						statistics,
						secondaryIndexType,
						fieldsForPartial));
				break;

			case TemporalSecondaryIndexConfiguration.INDEX_KEY:
				stat = new FeatureNumericHistogramStatistics(
						fieldName);
				statistics.add(stat);
				supportedSecondaryIndices.add(new SecondaryIndexImpl<>(
						new TemporalIndexStrategy(),
						fieldName,
						statistics,
						secondaryIndexType,
						fieldsForPartial));
				break;

			default:
				break;

		}
		for (final InternalDataStatistics<SimpleFeature, ?, ?> statistic : statistics) {
			statsManager.addStats(
					statistic,
					fieldName);
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * This consists of converting supported secondary indices.
	 */
	@Override
	public byte[] toBinary() {
		final List<Persistable> persistables = new ArrayList<>();
		for (final SecondaryIndexImpl<SimpleFeature> secondaryIndex : supportedSecondaryIndices) {
			persistables.add(secondaryIndex);
		}
		return PersistenceUtils.toBinary(persistables);
	}

	/**
	 * {@inheritDoc}
	 *
	 * This extracts the supported secondary indices from the binary stream and
	 * adds them in this object.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final List<Persistable> persistables = PersistenceUtils.fromBinaryAsList(bytes);
		for (final Persistable persistable : persistables) {
			supportedSecondaryIndices.add((SecondaryIndexImpl<SimpleFeature>) persistable);
		}
	}

	/**
	 *
	 * @return the StatsManager object being used by this SecondaryIndex.
	 */
	public StatsManager getStatsManager() {
		return statsManager;
	}
}
