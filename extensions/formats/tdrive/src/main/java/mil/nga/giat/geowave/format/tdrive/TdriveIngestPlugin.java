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
package mil.nga.giat.geowave.format.tdrive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.mortbay.log.Log;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;

/*
 */
public class TdriveIngestPlugin extends
		AbstractSimpleFeatureIngestPlugin<TdrivePoint>
{

	private final static Logger LOGGER = LoggerFactory.getLogger(TdriveIngestPlugin.class);

	private final SimpleFeatureBuilder tdrivepointBuilder;
	private final SimpleFeatureType tdrivepointType;

	private final ByteArrayId pointKey;

	public TdriveIngestPlugin() {

		tdrivepointType = TdriveUtils.createTdrivePointDataType();

		pointKey = new ByteArrayId(
				StringUtils.stringToBinary(TdriveUtils.TDRIVE_POINT_FEATURE));
		tdrivepointBuilder = new SimpleFeatureBuilder(
				tdrivepointType);
	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"csv",
			"txt"
		};
	}

	@Override
	public void init(
			final URL baseDirectory ) {

	}

	@Override
	public boolean supportsFile(
			final URL file ) {
		return TdriveUtils.validate(file);
	}

	@Override
	protected SimpleFeatureType[] getTypes() {
		return new SimpleFeatureType[] {
			SimpleFeatureUserDataConfigurationSet.configureType(tdrivepointType)
		};
	}

	@Override
	public Schema getAvroSchema() {
		return TdrivePoint.getClassSchema();
	}

	@Override
	public CloseableIterator<TdrivePoint> toAvroObjects(
			final URL input ) {
		try {
			final InputStream fis = input.openStream();
			final BufferedReader fr = new BufferedReader(
					new InputStreamReader(
							fis,
							StringUtils.GEOWAVE_CHAR_SET));
			final BufferedReader br = new BufferedReader(
					fr);
			return new CloseableIterator<TdrivePoint>() {
				TdrivePoint next = null;
				long pointInstance = 0l;

				private void computeNext() {
					if (next == null) {
						String line;
						try {
							if ((line = br.readLine()) != null) {
								final String[] vals = line.split(",");
								next = new TdrivePoint();
								next.setTaxiid(Integer.parseInt(vals[0]));
								try {
									next.setTimestamp(TdriveUtils.parseDate(
											vals[1]).getTime());
								}
								catch (final ParseException e) {
									next.setTimestamp(0l);
									LOGGER.warn(
											"Couldn't parse time format: " + vals[1],
											e);
								}
								next.setLongitude(Double.parseDouble(vals[2]));
								next.setLatitude(Double.parseDouble(vals[3]));
								next.setPointinstance(pointInstance);
								pointInstance++;
							}
						}
						catch (Exception e) {
							Log.warn(
									"Error parsing tdrive file: " + input.getPath(),
									e);
						}
					}
				}

				@Override
				public boolean hasNext() {
					computeNext();
					return next != null;
				}

				@Override
				public TdrivePoint next() {
					computeNext();
					TdrivePoint retVal = next;
					next = null;
					return retVal;
				}

				@Override
				public void close()
						throws IOException {
					br.close();
					fr.close();
					fis.close();

				}

			};
		}
		catch (final IOException e) {
			Log.warn(
					"Error parsing tdrive file: " + input.getPath(),
					e);
		}
		return new CloseableIterator.Empty<>();
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<TdrivePoint, SimpleFeature> ingestWithMapper() {
		return new IngestTdrivePointFromHdfs(
				this);
	}

	@Override
	public IngestWithReducer<TdrivePoint, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GPX tracks cannot be ingested with a reducer");
	}

	@Override
	protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final TdrivePoint tdrivePoint,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility ) {

		final List<GeoWaveData<SimpleFeature>> featureData = new ArrayList<GeoWaveData<SimpleFeature>>();

		// tdrivepointBuilder = new SimpleFeatureBuilder(tdrivepointType);
		tdrivepointBuilder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						tdrivePoint.getLongitude(),
						tdrivePoint.getLatitude())));
		tdrivepointBuilder.set(
				"taxiid",
				tdrivePoint.getTaxiid());
		tdrivepointBuilder.set(
				"pointinstance",
				tdrivePoint.getPointinstance());
		tdrivepointBuilder.set(
				"Timestamp",
				new Date(
						tdrivePoint.getTimestamp()));
		tdrivepointBuilder.set(
				"Latitude",
				tdrivePoint.getLatitude());
		tdrivepointBuilder.set(
				"Longitude",
				tdrivePoint.getLongitude());
		featureData.add(new GeoWaveData<SimpleFeature>(
				pointKey,
				primaryIndexIds,
				tdrivepointBuilder.buildFeature(tdrivePoint.getTaxiid() + "_" + tdrivePoint.getPointinstance())));

		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(
				featureData.iterator());
	}

	@Override
	public PrimaryIndex[] getRequiredIndices() {
		return new PrimaryIndex[] {};
	}

	public static class IngestTdrivePointFromHdfs extends
			AbstractIngestSimpleFeatureWithMapper<TdrivePoint>
	{
		public IngestTdrivePointFromHdfs() {
			this(
					new TdriveIngestPlugin());
			// this constructor will be used when deserialized
		}

		public IngestTdrivePointFromHdfs(
				final TdriveIngestPlugin parentPlugin ) {
			super(
					parentPlugin);
		}
	}

	@Override
	public IngestPluginBase<TdrivePoint, SimpleFeature> getIngestWithAvroPlugin() {
		return new IngestTdrivePointFromHdfs(
				this);
	}

	@Override
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}
}
