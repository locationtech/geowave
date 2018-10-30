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
package org.locationtech.geowave.format.gdelt;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.ZipInputStream;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import org.locationtech.geowave.adapter.vector.ingest.DataSchemaOptionProvider;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.geotime.store.dimension.Time;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.ingest.avro.WholeFile;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.IngestPluginBase;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/*
 */
public class GDELTIngestPlugin extends
		AbstractSimpleFeatureIngestPlugin<WholeFile>
{

	private final static Logger LOGGER = LoggerFactory.getLogger(GDELTIngestPlugin.class);

	private SimpleFeatureBuilder gdeltEventBuilder;
	private SimpleFeatureType gdeltEventType;

	private final String eventKey;

	private boolean includeSupplementalFields;

	public GDELTIngestPlugin() {

		// default to reduced data format
		setIncludeSupplementalFields(false);

		eventKey = GDELTUtils.GDELT_EVENT_FEATURE;
	}

	public GDELTIngestPlugin(
			final DataSchemaOptionProvider dataSchemaOptionProvider ) {
		setIncludeSupplementalFields(dataSchemaOptionProvider.includeSupplementalFields());
		eventKey = GDELTUtils.GDELT_EVENT_FEATURE;
	}

	private void setIncludeSupplementalFields(
			final boolean includeSupplementalFields ) {
		this.includeSupplementalFields = includeSupplementalFields;

		gdeltEventType = GDELTUtils.createGDELTEventDataType(includeSupplementalFields);
		gdeltEventBuilder = new SimpleFeatureBuilder(
				gdeltEventType);
	}

	@Override
	protected SimpleFeatureType[] getTypes() {
		return new SimpleFeatureType[] {
			SimpleFeatureUserDataConfigurationSet.configureType(gdeltEventType)
		};
	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"zip"
		};
	}

	@Override
	public void init(
			final URL baseDirectory ) {}

	@Override
	public boolean supportsFile(
			final URL file ) {
		return GDELTUtils.validate(file);
	}

	@Override
	public Schema getAvroSchema() {
		return WholeFile.getClassSchema();
	}

	@Override
	public CloseableIterator<WholeFile> toAvroObjects(
			final URL input ) {
		final WholeFile avroFile = new WholeFile();
		avroFile.setOriginalFilePath(input.getPath());
		try {
			avroFile.setOriginalFile(ByteBuffer.wrap(IOUtils.toByteArray(input)));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read GDELT file: " + input.getPath(),
					e);
			return new CloseableIterator.Empty<>();
		}

		return new CloseableIterator.Wrapper<>(
				Iterators.singletonIterator(avroFile));
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<WholeFile, SimpleFeature> ingestWithMapper() {
		return new IngestGDELTFromHdfs(
				this);
	}

	@Override
	public IngestWithReducer<WholeFile, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GDELT events cannot be ingested with a reducer");
	}

	@Override
	@SuppressFBWarnings(value = {
		"REC_CATCH_EXCEPTION"
	}, justification = "Intentionally catching any possible exception as there may be unknown format issues in a file and we don't want to error partially through parsing")
	protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final WholeFile hfile,
			final String[] indexNames,
			final String globalVisibility ) {

		final List<GeoWaveData<SimpleFeature>> featureData = new ArrayList<>();

		final InputStream in = new ByteArrayInputStream(
				hfile.getOriginalFile().array());
		final ZipInputStream zip = new ZipInputStream(
				in);
		try {
			// Expected input is zipped single files (exactly one entry)
			zip.getNextEntry();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Failed to read ZipEntry from GDELT input file: " + hfile.getOriginalFilePath(),
					e);
		}

		final InputStreamReader isr = new InputStreamReader(
				zip,
				StringUtils.UTF8_CHARSET);
		final BufferedReader br = new BufferedReader(
				isr);

		final GeometryFactory geometryFactory = new GeometryFactory();

		Date timeStamp = null;
		String timestring = "";
		String eventId = "";
		int actionGeoType;
		double lat = 0;
		double lon = 0;
		String actor1Name = "";
		String actor2Name = "";
		String countryCode = "";
		String sourceUrl = "";
		String actor1CC = "";
		String actor2CC = "";
		String numMentions = "";
		String numSources = "";
		String numArticles = "";
		String avgTone = "";

		String line;
		int lineNumber = 0;
		try {
			while ((line = br.readLine()) != null) {
				lineNumber++;
				try {
					final String[] vals = line.split("\t");
					if ((vals.length < GDELTUtils.GDELT_MIN_COLUMNS) || (vals.length > GDELTUtils.GDELT_MAX_COLUMNS)) {
						LOGGER.debug("Invalid GDELT line length: " + vals.length + " tokens found on line "
								+ lineNumber + " of " + hfile.getOriginalFilePath());
						continue;
					}

					actionGeoType = Integer.parseInt(vals[GDELTUtils.GDELT_ACTION_GEO_TYPE_COLUMN_ID]);
					if (actionGeoType == 0) {
						// No geo associated with this event
						continue;
					}

					eventId = vals[GDELTUtils.GDELT_EVENT_ID_COLUMN_ID];

					try {
						final Pair<Double, Double> latLon = GDELTUtils.parseLatLon(vals);
						if (latLon == null) {
							LOGGER
									.debug("No spatial data on line " + lineNumber + " of "
											+ hfile.getOriginalFilePath());
							continue;
						}
						lat = latLon.getLeft();
						lon = latLon.getRight();
					}
					catch (final Exception e) {
						LOGGER.debug(
								"Error reading GDELT lat/lon on line " + lineNumber + " of "
										+ hfile.getOriginalFilePath(),
								e);
						continue;
					}

					final Coordinate cord = new Coordinate(
							lon,
							lat);

					gdeltEventBuilder.set(
							GDELTUtils.GDELT_GEOMETRY_ATTRIBUTE,
							geometryFactory.createPoint(cord));

					gdeltEventBuilder.set(
							GDELTUtils.GDELT_EVENT_ID_ATTRIBUTE,
							eventId);

					timestring = vals[GDELTUtils.GDELT_TIMESTAMP_COLUMN_ID];
					timeStamp = GDELTUtils.parseDate(timestring);
					gdeltEventBuilder.set(
							GDELTUtils.GDELT_TIMESTAMP_ATTRIBUTE,
							timeStamp);

					gdeltEventBuilder.set(
							GDELTUtils.GDELT_LATITUDE_ATTRIBUTE,
							lat);
					gdeltEventBuilder.set(
							GDELTUtils.GDELT_LONGITUDE_ATTRIBUTE,
							lon);

					actor1Name = vals[GDELTUtils.ACTOR_1_NAME_COLUMN_ID];
					if ((actor1Name != null) && !actor1Name.isEmpty()) {
						gdeltEventBuilder.set(
								GDELTUtils.ACTOR_1_NAME_ATTRIBUTE,
								actor1Name);
					}

					actor2Name = vals[GDELTUtils.ACTOR_2_NAME_COLUMN_ID];
					if ((actor2Name != null) && !actor2Name.isEmpty()) {
						gdeltEventBuilder.set(
								GDELTUtils.ACTOR_2_NAME_ATTRIBUTE,
								actor2Name);
					}

					countryCode = vals[GDELTUtils.ACTION_COUNTRY_CODE_COLUMN_ID];
					if ((countryCode != null) && !countryCode.isEmpty()) {
						gdeltEventBuilder.set(
								GDELTUtils.ACTION_COUNTRY_CODE_ATTRIBUTE,
								countryCode);
					}
					if (vals.length > GDELTUtils.SOURCE_URL_COLUMN_ID) {
						sourceUrl = vals[GDELTUtils.SOURCE_URL_COLUMN_ID];
					}
					if ((sourceUrl != null) && !sourceUrl.isEmpty()) {
						gdeltEventBuilder.set(
								GDELTUtils.SOURCE_URL_ATTRIBUTE,
								sourceUrl);
					}

					if (includeSupplementalFields) {

						actor1CC = vals[GDELTUtils.ACTOR_1_COUNTRY_CODE_COLUMN_ID];
						if ((actor1CC != null) && !actor1CC.isEmpty()) {
							gdeltEventBuilder.set(
									GDELTUtils.ACTOR_1_COUNTRY_CODE_ATTRIBUTE,
									actor1CC);
						}

						actor2CC = vals[GDELTUtils.ACTOR_2_COUNTRY_CODE_COLUMN_ID];
						if ((actor2CC != null) && !actor2CC.isEmpty()) {
							gdeltEventBuilder.set(
									GDELTUtils.ACTOR_2_COUNTRY_CODE_ATTRIBUTE,
									actor2CC);
						}

						numMentions = vals[GDELTUtils.NUM_MENTIONS_COLUMN_ID];
						if ((numMentions != null) && !numMentions.isEmpty()) {
							gdeltEventBuilder.set(
									GDELTUtils.NUM_MENTIONS_ATTRIBUTE,
									Integer.parseInt(numMentions));
						}

						numSources = vals[GDELTUtils.NUM_SOURCES_COLUMN_ID];
						if ((numSources != null) && !numSources.isEmpty()) {
							gdeltEventBuilder.set(
									GDELTUtils.NUM_SOURCES_ATTRIBUTE,
									Integer.parseInt(numSources));
						}

						numArticles = vals[GDELTUtils.NUM_ARTICLES_COLUMN_ID];
						if ((numArticles != null) && !numArticles.isEmpty()) {
							gdeltEventBuilder.set(
									GDELTUtils.NUM_ARTICLES_ATTRIBUTE,
									Integer.parseInt(numArticles));
						}

						avgTone = vals[GDELTUtils.AVG_TONE_COLUMN_ID];
						if ((avgTone != null) && !avgTone.isEmpty()) {
							gdeltEventBuilder.set(
									GDELTUtils.AVG_TONE_ATTRIBUTE,
									Double.parseDouble(avgTone));
						}
					}

					featureData.add(new GeoWaveData<>(
							eventKey,
							indexNames,
							gdeltEventBuilder.buildFeature(eventId)));
				}
				catch (final Exception e) {

					LOGGER.error(
							"Error parsing line: " + line,
							e);
					continue;
				}
			}

		}
		catch (final IOException e) {
			LOGGER.warn(
					"Error reading line from GDELT file: " + hfile.getOriginalFilePath(),
					e);
		}
		finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(isr);
			IOUtils.closeQuietly(in);
		}

		return new CloseableIterator.Wrapper<>(
				featureData.iterator());
	}

	@Override
	public Index[] getRequiredIndices() {
		return new Index[] {};
	}

	@Override
	public IngestPluginBase<WholeFile, SimpleFeature> getIngestWithAvroPlugin() {
		return new IngestGDELTFromHdfs(
				this);
	}

	public static class IngestGDELTFromHdfs extends
			AbstractIngestSimpleFeatureWithMapper<WholeFile>
	{
		public IngestGDELTFromHdfs() {
			this(
					new GDELTIngestPlugin());
		}

		public IngestGDELTFromHdfs(
				final GDELTIngestPlugin parentPlugin ) {
			super(
					parentPlugin);
		}
	}

	@Override
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}
}
