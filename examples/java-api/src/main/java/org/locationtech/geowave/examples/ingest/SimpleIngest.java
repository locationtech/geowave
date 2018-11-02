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
package org.locationtech.geowave.examples.ingest;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider.SpatialTemporalIndexBuilder;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class SimpleIngest
{
	public static final String FEATURE_NAME = "GridPoint";
	private static Logger log = LoggerFactory.getLogger(SimpleIngest.class);

	public static void main(
			final String[] args ) {
		final SimpleIngest si = new SimpleIngest();
		final DataStore geowaveDataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());

		si.writeExampleData(geowaveDataStore);
		System.out.println("Finished ingesting data");
	}

	/***
	 * Here we will change the ingest mechanism to use a producer/consumer
	 * pattern
	 */
	protected void writeExampleData(
			final DataStore geowaveDataStore ) {

		// In order to store data we need to determine the type of data store
		final SimpleFeatureType point = createPointFeatureType();

		// This a factory class that builds simple feature objects based on the
		// type passed
		final SimpleFeatureBuilder pointBuilder = new SimpleFeatureBuilder(
				point);

		// This is an adapter, that is needed to describe how to persist the
		// data type passed
		final GeotoolsFeatureDataAdapter dataTypeAdapter = createDataAdapter(point);

		// This describes how to index the data
		final Index index = createSpatialIndex();
		geowaveDataStore.addType(
				dataTypeAdapter,
				index);
		// make sure to close the index writer (a try-with-resources block such
		// as this automatically closes the resource when exiting the block)
		try (Writer<SimpleFeature> indexWriter = geowaveDataStore.createWriter(dataTypeAdapter.getTypeName())) {
			// build a grid of points across the globe at each whole
			// lattitude/longitude intersection

			for (final SimpleFeature sft : getGriddedFeatures(
					pointBuilder,
					1000)) {
				indexWriter.write(sft);
			}
		}
	}

	public static List<SimpleFeature> getGriddedFeatures(
			final SimpleFeatureBuilder pointBuilder,
			final int firstFeatureId ) {

		// features require a featureID - this should be uniqiue per data type
		// adapter ID
		// (i.e. writing a new feature with the same feature id for the same
		// data type adapter will
		// overwrite the existing feature)
		int featureId = firstFeatureId;
		final List<SimpleFeature> feats = new ArrayList<>();
		for (int longitude = -180; longitude <= 180; longitude += 5) {
			for (int latitude = -90; latitude <= 90; latitude += 5) {
				pointBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								longitude,
								latitude)));
				pointBuilder.set(
						"TimeStamp",
						new Date());
				pointBuilder.set(
						"Latitude",
						latitude);
				pointBuilder.set(
						"Longitude",
						longitude);
				// Note since trajectoryID and comment are marked as nillable we
				// don't need to set them (they default ot null).

				final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				feats.add(sft);
				featureId++;
			}
		}
		return feats;
	}

	/***
	 * The dataadapter interface describes how to serialize a data type. Here we
	 * are using an implementation that understands how to serialize OGC
	 * SimpleFeature types.
	 *
	 * @param sft
	 *            simple feature type you want to generate an adapter from
	 * @return data adapter that handles serialization of the sft simple feature
	 *         type
	 */
	public static GeotoolsFeatureDataAdapter createDataAdapter(
			final SimpleFeatureType sft ) {
		return new FeatureDataAdapter(
				sft);
	}

	/***
	 * We need an index model that tells us how to index the data - the index
	 * determines -What fields are indexed -The precision of the index -The
	 * range of the index (min/max values) -The range type (bounded/unbounded)
	 * -The number of "levels" (different precisions, needed when the values
	 * indexed has ranges on any dimension)
	 *
	 * @return GeoWave index for a default SPATIAL index
	 */
	public static Index createSpatialIndex() {

		// Reasonable values for spatial and spatial-temporal are provided
		// through index builders.
		// They are intended to be a reasonable starting place - though creating
		// a custom index may provide better
		// performance as the distribution/characterization of the data is well
		// known. There are many such customizations available through setters
		// on the builder.

		// for example to create a spatial-temporal index with 8 randomized
		// partitions (pre-splits on accumulo or hbase) and a temporal bias
		// (giving more precision to time than space) you could do something
		// like this:
		//@formatter:off
		// return new SpatialTemporalIndexBuilder().setBias(Bias.TEMPORAL).setNumPartitions(8);
		//@formatter:on
		return new SpatialIndexBuilder().createIndex();
	}

	public static Index createSpatialTemporalIndex() {
		return new SpatialTemporalIndexBuilder().createIndex();
	}

	/***
	 * A simple feature is just a mechanism for defining attributes (a feature
	 * is just a collection of attributes + some metadata) We need to describe
	 * what our data looks like so the serializer (FeatureDataAdapter for this
	 * case) can know how to store it. Features/Attributes are also a general
	 * convention of GIS systems in general.
	 *
	 * @return Simple Feature definition for our demo point feature
	 */
	public static SimpleFeatureType createPointFeatureType() {

		final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		final AttributeTypeBuilder ab = new AttributeTypeBuilder();

		// Names should be unique (at least for a given GeoWave namespace) -
		// think about names in the same sense as a full classname
		// The value you set here will also persist through discovery - so when
		// people are looking at a dataset they will see the
		// type names associated with the data.
		builder.setName(FEATURE_NAME);

		// The data is persisted in a sparse format, so if data is nullable it
		// will not take up any space if no values are persisted.
		// Data which is included in the primary index (in this example
		// lattitude/longtiude) can not be null
		// Calling out latitude an longitude separately is not strictly needed,
		// as the geometry contains that information. But it's
		// convienent in many use cases to get a text representation without
		// having to handle geometries.
		builder.add(ab.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				"geometry"));
		builder.add(ab.binding(
				Date.class).nillable(
				true).buildDescriptor(
				"TimeStamp"));
		builder.add(ab.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"Latitude"));
		builder.add(ab.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"Longitude"));
		builder.add(ab.binding(
				String.class).nillable(
				true).buildDescriptor(
				"TrajectoryID"));
		builder.add(ab.binding(
				String.class).nillable(
				true).buildDescriptor(
				"Comment"));

		return builder.buildFeatureType();
	}

}
