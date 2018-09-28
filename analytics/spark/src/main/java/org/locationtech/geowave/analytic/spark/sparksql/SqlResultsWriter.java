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
package org.locationtech.geowave.analytic.spark.sparksql;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.util.Date;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.analytic.spark.sparksql.util.SchemaConverter;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

public class SqlResultsWriter
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SqlResultsWriter.class);

	private static final String DEFAULT_TYPE_NAME = "sqlresults";

	private final Dataset<Row> results;
	private final DataStorePluginOptions outputDataStore;
	private final NumberFormat nf;

	public SqlResultsWriter(
			Dataset<Row> results,
			DataStorePluginOptions outputDataStore ) {
		this.results = results;
		this.outputDataStore = outputDataStore;

		this.nf = NumberFormat.getIntegerInstance();
		this.nf.setMinimumIntegerDigits(6);
	}

	public void writeResults(
			String typeName ) {
		if (typeName == null) {
			typeName = DEFAULT_TYPE_NAME;
			LOGGER.warn("Using default type name (adapter id): '" + DEFAULT_TYPE_NAME + "' for SQL output");
		}

		StructType schema = results.schema();
		SimpleFeatureType featureType = SchemaConverter.schemaToFeatureType(
				schema,
				typeName);

		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				featureType);

		final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
				featureType);

		final DataStore featureStore = outputDataStore.createDataStore();
		final Index featureIndex = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());

		try (Writer writer = featureStore.createWriter(
				featureAdapter,
				featureIndex)) {

			List<Row> rows = results.collectAsList();

			for (int r = 0; r < rows.size(); r++) {
				Row row = rows.get(r);

				for (int i = 0; i < schema.fields().length; i++) {
					StructField field = schema.apply(i);
					Object rowObj = row.apply(i);
					if (rowObj != null) {
						if (field.name().equals(
								"geom")) {
							Geometry geom = (Geometry) rowObj;

							sfBuilder.set(
									"geom",
									geom);
						}
						else if (field.dataType() == DataTypes.TimestampType) {
							long millis = ((Timestamp) rowObj).getTime();
							Date date = new Date(
									millis);

							sfBuilder.set(
									field.name(),
									date);
						}
						else {
							sfBuilder.set(
									field.name(),
									rowObj);
						}
					}
				}

				final SimpleFeature sf = sfBuilder.buildFeature("result-" + nf.format(r));

				writer.write(sf);
			}
		}
		catch (final MismatchedIndexToAdapterMapping e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}
	}
}
