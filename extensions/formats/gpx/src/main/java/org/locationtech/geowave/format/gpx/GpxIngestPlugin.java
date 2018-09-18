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
package org.locationtech.geowave.format.gpx;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.stream.XMLStreamException;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.geotime.store.dimension.Time;
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
import org.xml.sax.SAXException;

import jersey.repackaged.com.google.common.collect.Iterators;

/**
 * This plugin is used for ingesting any GPX formatted data from a local file
 * system into GeoWave as GeoTools' SimpleFeatures. It supports the default
 * configuration of spatial and spatial-temporal indices and it will support
 * wither directly ingesting GPX data from a local file system to GeoWave or to
 * stage the data in an intermediate format in HDFS and then to ingest it into
 * GeoWave using a map-reduce job. It supports OSM metadata.xml files if the
 * file is directly in the root base directory that is passed in command-line to
 * the ingest framework.
 */
public class GpxIngestPlugin extends
		AbstractSimpleFeatureIngestPlugin<GpxTrack>
{

	private final static Logger LOGGER = LoggerFactory.getLogger(GpxIngestPlugin.class);

	private final static String TAG_SEPARATOR = " ||| ";

	private Map<Long, GpxTrack> metadata = null;

	private MaxExtentOptProvider extentOptProvider = new MaxExtentOptProvider();

	private static final AtomicLong currentFreeTrackId = new AtomicLong(
			0);

	public GpxIngestPlugin() {}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"xml",
			"gpx"
		};
	}

	@Override
	public void init(
			final URL baseDirectory ) {
		URL f = null;
		try {
			f = new URL(
					baseDirectory.toString().concat(
							"metadata.xml"));
		}
		catch (final MalformedURLException e1) {
			LOGGER.info(
					"Invalid URL for metadata.xml. No metadata will be loaded",
					e1);
		}
		if (f != null) {
			try {
				long time = System.currentTimeMillis();
				metadata = GpxUtils.parseOsmMetadata(f);
				time = System.currentTimeMillis() - time;
				final String timespan = String.format(
						"%d min, %d sec",
						TimeUnit.MILLISECONDS.toMinutes(time),
						TimeUnit.MILLISECONDS.toSeconds(time)
								- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time)));
				LOGGER.info("Metadata parsed in in " + timespan + " for " + metadata.size() + " tracks");
			}
			catch (final XMLStreamException | FileNotFoundException e) {
				LOGGER.warn(
						"Unable to read OSM metadata file: " + f.getPath(),
						e);
			}
		}

	}

	@Override
	public boolean supportsFile(
			final URL file ) {
		// if its a gpx extension assume it is supported
		if (FilenameUtils.getName(
				file.getPath()).toLowerCase(
				Locale.ENGLISH).endsWith(
				"gpx")) {
			return true;
		}
		if ("metadata.xml".equals(FilenameUtils.getName(file.getPath()))) {
			return false;
		}
		// otherwise take a quick peek at the file to ensure it matches the GPX
		// schema
		try {
			return GpxUtils.validateGpx(file);
		}
		catch (SAXException | IOException e) {
			LOGGER.warn(
					"Unable to read file:" + file.getPath(),
					e);
		}
		return false;
	}

	@Override
	protected SimpleFeatureType[] getTypes() {
		return new SimpleFeatureType[] {
			SimpleFeatureUserDataConfigurationSet.configureType(GPXConsumer.pointType),
			SimpleFeatureUserDataConfigurationSet.configureType(GPXConsumer.waypointType),
			SimpleFeatureUserDataConfigurationSet.configureType(GPXConsumer.trackType),
			SimpleFeatureUserDataConfigurationSet.configureType(GPXConsumer.routeType)
		};
	}

	@Override
	public Schema getAvroSchema() {
		return GpxTrack.getClassSchema();
	}

	@Override
	public CloseableIterator<GpxTrack> toAvroObjects(
			final URL input ) {
		GpxTrack track = null;
		if (metadata != null) {
			try {
				final long id = Long.parseLong(FilenameUtils.getBaseName(input.getPath()));
				track = metadata.remove(id);
			}
			catch (final NumberFormatException e) {
				LOGGER.info("OSM metadata found, but track file name is not a numeric ID");
			}
		}
		if (track == null) {
			track = new GpxTrack();
			track.setTrackid(currentFreeTrackId.getAndIncrement());
		}

		try {
			track.setGpxfile(ByteBuffer.wrap(IOUtils.toByteArray(input)));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read GPX file: " + input.getPath(),
					e);
		}

		return new CloseableIterator.Wrapper<>(
				Iterators.singletonIterator(track));
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<GpxTrack, SimpleFeature> ingestWithMapper() {
		return new IngestGpxTrackFromHdfs(
				this);
	}

	@Override
	public IngestWithReducer<GpxTrack, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GPX tracks cannot be ingested with a reducer");
	}

	@Override
	protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final GpxTrack gpxTrack,
			final String[] indexNames,
			final String globalVisibility ) {
		final InputStream in = new ByteArrayInputStream(
				gpxTrack.getGpxfile().array());
		// LOGGER.debug("Processing track [" + gpxTrack.getTimestamp() + "]");
		try {
			return new GPXConsumer(
					in,
					indexNames,
					gpxTrack.getTrackid() == null ? "" : gpxTrack.getTrackid().toString(),
					getAdditionalData(gpxTrack),
					false, // waypoints, even dups, are unique, due to QGis
							// behavior
					globalVisibility,
					extentOptProvider.getMaxExtent());
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to convert GpxTrack to GeoWaveData",
					e);
			return null;
		}
	}

	@Override
	public Index[] getRequiredIndices() {
		return new Index[] {};
	}

	private Map<String, Map<String, String>> getAdditionalData(
			final GpxTrack gpxTrack ) {
		final Map<String, Map<String, String>> pathDataSet = new HashMap<>();
		final Map<String, String> dataSet = new HashMap<>();
		pathDataSet.put(
				"gpx.trk",
				dataSet);

		if (gpxTrack.getTrackid() != null) {
			dataSet.put(
					"TrackId",
					gpxTrack.getTrackid().toString());
		}
		if (gpxTrack.getUserid() != null) {
			dataSet.put(
					"UserId",
					gpxTrack.getUserid().toString());
		}
		if (gpxTrack.getUser() != null) {
			dataSet.put(
					"User",
					gpxTrack.getUser().toString());
		}
		if (gpxTrack.getDescription() != null) {
			dataSet.put(
					"Description",
					gpxTrack.getDescription().toString());
		}

		if ((gpxTrack.getTags() != null) && (gpxTrack.getTags().size() > 0)) {
			final String tags = org.apache.commons.lang.StringUtils.join(
					gpxTrack.getTags(),
					TAG_SEPARATOR);
			dataSet.put(
					"Tags",
					tags);
		}
		else {
			dataSet.put(
					"Tags",
					null);
		}
		return pathDataSet;
	}

	public static class IngestGpxTrackFromHdfs extends
			AbstractIngestSimpleFeatureWithMapper<GpxTrack>
	{
		public IngestGpxTrackFromHdfs() {
			this(
					new GpxIngestPlugin());
			// this constructor will be used when deserialized
		}

		public IngestGpxTrackFromHdfs(
				final GpxIngestPlugin parentPlugin ) {
			super(
					parentPlugin);
		}
	}

	@Override
	public IngestPluginBase<GpxTrack, SimpleFeature> getIngestWithAvroPlugin() {
		return new IngestGpxTrackFromHdfs(
				this);
	}

	@Override
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}

	public void setExtentOptionProvider(
			final MaxExtentOptProvider extentOptProvider ) {
		this.extentOptProvider = extentOptProvider;
	}

	public MaxExtentOptProvider getExtentOptionProvider() {
		return extentOptProvider;
	}
}
