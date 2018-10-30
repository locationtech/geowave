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
package org.locationtech.geowave.examples.ingest.bulk;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.InitializeWithIndicesDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.UniformVisibilityWriter;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.datastore.accumulo.util.AccumuloKeyValuePairGenerator;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;

public class SimpleFeatureToAccumuloKeyValueMapper extends
		Mapper<LongWritable, Text, Key, Value>
{

	private final DataTypeAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
			GeonamesSimpleFeatureType.getInstance());
	private final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
	private final VisibilityWriter<SimpleFeature> visibilityWriter = new UniformVisibilityWriter<SimpleFeature>(
			new UnconstrainedVisibilityHandler<SimpleFeature, Object>());
	private final AccumuloKeyValuePairGenerator<SimpleFeature> generator = new AccumuloKeyValuePairGenerator<SimpleFeature>(
			// this is not the most robust way to assign an internal adapter ID
			// but is simple and will work in a majority of cases
			new InternalDataAdapterWrapper<>(
					adapter,
					InternalAdapterStoreImpl.getInitialAdapterId(adapter.getTypeName())),
			index,
			visibilityWriter);
	private SimpleFeature simpleFeature;
	private List<KeyValue> keyValuePairs;
	private final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
			GeonamesSimpleFeatureType.getInstance());
	private String[] geonamesEntryTokens;
	private String geonameId;
	private double longitude;
	private double latitude;
	private String location;

	@Override
	protected void map(
			final LongWritable key,
			final Text value,
			final Context context )
			throws IOException,
			InterruptedException {

		simpleFeature = parseGeonamesValue(value);
		((InitializeWithIndicesDataAdapter) adapter).init(index);

		// build Geowave-formatted Accumulo [Key,Value] pairs
		keyValuePairs = generator.constructKeyValuePairs(simpleFeature);

		// output each [Key,Value] pair to shuffle-and-sort phase where we rely
		// on MapReduce to sort by Key
		for (final KeyValue accumuloKeyValuePair : keyValuePairs) {
			context.write(
					accumuloKeyValuePair.getKey(),
					accumuloKeyValuePair.getValue());
		}
	}

	private SimpleFeature parseGeonamesValue(
			final Text value ) {

		geonamesEntryTokens = value.toString().split(
				"\\t"); // Exported Geonames entries are tab-delimited

		geonameId = geonamesEntryTokens[0];
		location = geonamesEntryTokens[1];
		latitude = Double.parseDouble(geonamesEntryTokens[4]);
		longitude = Double.parseDouble(geonamesEntryTokens[5]);

		return buildSimpleFeature(
				geonameId,
				longitude,
				latitude,
				location);
	}

	private SimpleFeature buildSimpleFeature(
			final String featureId,
			final double longitude,
			final double latitude,
			final String location ) {

		builder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						longitude,
						latitude)));
		builder.set(
				"Latitude",
				latitude);
		builder.set(
				"Longitude",
				longitude);
		builder.set(
				"Location",
				location);

		return builder.buildFeature(featureId);
	}

}
