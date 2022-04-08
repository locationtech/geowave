/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.geolife;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.ingest.avro.AvroWholeFile;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.IngestPluginBase;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jersey.repackaged.com.google.common.collect.Iterators;

/*
 */
public class GeoLifeIngestPlugin extends AbstractSimpleFeatureIngestPlugin<AvroWholeFile> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoLifeIngestPlugin.class);

  private final SimpleFeatureBuilder geolifePointBuilder;
  private final SimpleFeatureType geolifePointType;

  private final SimpleFeatureBuilder geolifeTrackBuilder;
  private final SimpleFeatureType geolifeTrackType;

  private final String pointKey;
  private final String trackKey;

  private CoordinateReferenceSystem crs;

  public GeoLifeIngestPlugin() {
    geolifePointType = GeoLifeUtils.createGeoLifePointDataType();
    pointKey = GeoLifeUtils.GEOLIFE_POINT_FEATURE;
    geolifePointBuilder = new SimpleFeatureBuilder(geolifePointType);

    geolifeTrackType = GeoLifeUtils.createGeoLifeTrackDataType();
    trackKey = GeoLifeUtils.GEOLIFE_TRACK_FEATURE;
    geolifeTrackBuilder = new SimpleFeatureBuilder(geolifeTrackType);
    try {
      crs = CRS.decode("EPSG:4326");
    } catch (final FactoryException e) {
      LOGGER.error("Unable to decode Coordinate Reference System authority code!", e);
    }
  }

  @Override
  protected SimpleFeatureType[] getTypes() {
    return new SimpleFeatureType[] {
        SimpleFeatureUserDataConfigurationSet.configureType(geolifePointType),
        SimpleFeatureUserDataConfigurationSet.configureType(geolifeTrackType)};
  }

  @Override
  public String[] getFileExtensionFilters() {
    return new String[] {"plt"};
  }

  @Override
  public void init(final URL baseDirectory) {}

  @Override
  public boolean supportsFile(final URL file) {
    return GeoLifeUtils.validate(file);
  }

  @Override
  public Schema getAvroSchema() {
    return AvroWholeFile.getClassSchema();
  }

  @Override
  public CloseableIterator<AvroWholeFile> toAvroObjects(final URL input) {
    final AvroWholeFile avroFile = new AvroWholeFile();
    avroFile.setOriginalFilePath(input.getPath());
    try {
      avroFile.setOriginalFile(ByteBuffer.wrap(IOUtils.toByteArray(input)));
    } catch (final IOException e) {
      LOGGER.warn("Unable to read GeoLife file: " + input.getPath(), e);
      return new CloseableIterator.Empty<>();
    }

    return new CloseableIterator.Wrapper<>(Iterators.singletonIterator(avroFile));
  }

  @Override
  public boolean isUseReducerPreferred() {
    return false;
  }

  @Override
  public IngestWithMapper<AvroWholeFile, SimpleFeature> ingestWithMapper() {
    return new IngestGeoLifeFromHdfs(this);
  }

  @Override
  public IngestWithReducer<AvroWholeFile, ?, ?, SimpleFeature> ingestWithReducer() {
    // unsupported right now
    throw new UnsupportedOperationException("GeoLife tracks cannot be ingested with a reducer");
  }

  @Override
  protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
      final AvroWholeFile hfile,
      final String[] indexNames) {

    final List<GeoWaveData<SimpleFeature>> featureData = new ArrayList<>();

    final InputStream in = new ByteArrayInputStream(hfile.getOriginalFile().array());
    final InputStreamReader isr = new InputStreamReader(in, StringUtils.getGeoWaveCharset());
    final BufferedReader br = new BufferedReader(isr);
    int pointInstance = 0;
    final List<Coordinate> pts = new ArrayList<>();
    final String trackId = FilenameUtils.getName(hfile.getOriginalFilePath().toString());
    String line;
    Date startTimeStamp = null;
    Date endTimeStamp = null;
    String timestring = "";
    final GeometryFactory geometryFactory = new GeometryFactory();
    double currLat;
    double currLng;
    try {
      while ((line = br.readLine()) != null) {

        final String[] vals = line.split(",");
        if (vals.length != 7) {
          continue;
        }

        currLat =
            GeometryUtils.adjustCoordinateDimensionToRange(Double.parseDouble(vals[0]), crs, 1);
        currLng =
            GeometryUtils.adjustCoordinateDimensionToRange(Double.parseDouble(vals[1]), crs, 0);
        final Coordinate cord = new Coordinate(currLng, currLat);
        pts.add(cord);
        geolifePointBuilder.set("geometry", geometryFactory.createPoint(cord));
        geolifePointBuilder.set("trackid", trackId);
        geolifePointBuilder.set("pointinstance", pointInstance);
        pointInstance++;

        timestring = vals[5] + " " + vals[6];
        final Date ts = GeoLifeUtils.parseDate(timestring);
        geolifePointBuilder.set("Timestamp", ts);
        if (startTimeStamp == null) {
          startTimeStamp = ts;
        }
        endTimeStamp = ts;

        geolifePointBuilder.set("Latitude", currLat);
        geolifePointBuilder.set("Longitude", currLng);

        Double elevation = Double.parseDouble(vals[3]);
        if (elevation == -777) {
          elevation = null;
        }
        geolifePointBuilder.set("Elevation", elevation);
        featureData.add(
            new GeoWaveData<>(
                pointKey,
                indexNames,
                geolifePointBuilder.buildFeature(trackId + "_" + pointInstance)));
      }

      geolifeTrackBuilder.set(
          "geometry",
          geometryFactory.createLineString(pts.toArray(new Coordinate[pts.size()])));

      geolifeTrackBuilder.set("StartTimeStamp", startTimeStamp);
      geolifeTrackBuilder.set("EndTimeStamp", endTimeStamp);
      if ((endTimeStamp != null) && (startTimeStamp != null)) {
        geolifeTrackBuilder.set("Duration", endTimeStamp.getTime() - startTimeStamp.getTime());
      }
      geolifeTrackBuilder.set("NumberPoints", pointInstance);
      geolifeTrackBuilder.set("TrackId", trackId);
      featureData.add(
          new GeoWaveData<>(trackKey, indexNames, geolifeTrackBuilder.buildFeature(trackId)));

    } catch (final IOException e) {
      LOGGER.warn("Error reading line from file: " + hfile.getOriginalFilePath(), e);
    } catch (final ParseException e) {
      LOGGER.error("Error parsing time string: " + timestring, e);
    } finally {
      IOUtils.closeQuietly(br);
      IOUtils.closeQuietly(isr);
      IOUtils.closeQuietly(in);
    }

    return new CloseableIterator.Wrapper<>(featureData.iterator());
  }

  @Override
  public Index[] getRequiredIndices() {
    return new Index[] {};
  }

  @Override
  public IngestPluginBase<AvroWholeFile, SimpleFeature> getIngestWithAvroPlugin() {
    return new IngestGeoLifeFromHdfs(this);
  }

  public static class IngestGeoLifeFromHdfs extends
      AbstractIngestSimpleFeatureWithMapper<AvroWholeFile> {
    public IngestGeoLifeFromHdfs() {
      this(new GeoLifeIngestPlugin());
    }

    public IngestGeoLifeFromHdfs(final GeoLifeIngestPlugin parentPlugin) {
      super(parentPlugin);
    }
  }

  @Override
  public String[] getSupportedIndexTypes() {
    return new String[] {SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, TimeField.DEFAULT_FIELD_ID};
  }
}
