/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676;

import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.jaitools.jts.CoordinateSequence2D;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.FloatCompareUtils;
import org.locationtech.geowave.core.ingest.avro.AbstractStageWholeFileToAvro;
import org.locationtech.geowave.core.ingest.avro.AvroWholeFile;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.KeyValueData;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.IngestPluginBase;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.format.stanag4676.image.ImageChip;
import org.locationtech.geowave.format.stanag4676.image.ImageChipDataAdapter;
import org.locationtech.geowave.format.stanag4676.parser.NATO4676Decoder;
import org.locationtech.geowave.format.stanag4676.parser.TrackFileReader;
import org.locationtech.geowave.format.stanag4676.parser.util.EarthVector;
import org.locationtech.geowave.format.stanag4676.parser.util.Length;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;

public class Stanag4676IngestPlugin extends AbstractStageWholeFileToAvro<Object> implements
    IngestFromHdfsPlugin<AvroWholeFile, Object>,
    LocalFileIngestPlugin<Object> {
  private static Logger LOGGER = LoggerFactory.getLogger(Stanag4676IngestPlugin.class);
  public static final Index IMAGE_CHIP_INDEX = new NullIndex("IMAGERY_CHIPS");

  private static final String[] IMAGE_CHIP_AS_ARRAY = new String[] {IMAGE_CHIP_INDEX.getName()};

  @Override
  public String[] getFileExtensionFilters() {
    return new String[] {"xml", "4676"};
  }

  public Stanag4676IngestPlugin() {
    super();
  }

  @Override
  public void init(final URL baseDirectory) {}

  @Override
  public boolean supportsFile(final URL file) {
    // TODO: consider checking for schema compliance
    try {
      return file.openConnection().getContentLength() > 0;
    } catch (final IOException e) {
      LOGGER.info("Unable to read URL for '" + file.getPath() + "'", e);
    }
    return false;
  }

  @Override
  public boolean isUseReducerPreferred() {
    return true;
  }

  @Override
  public IngestWithMapper<AvroWholeFile, Object> ingestWithMapper() {
    return new IngestWithReducerImpl();
  }

  @Override
  public IngestWithReducer<AvroWholeFile, ?, ?, Object> ingestWithReducer() {
    return new IngestWithReducerImpl();
  }

  @Override
  public CloseableIterator<GeoWaveData<Object>> toGeoWaveData(
      final URL file,
      final String[] indexNames) {
    return ingestWithMapper().toGeoWaveData(toAvroObjects(file).next(), indexNames);
  }

  @Override
  public DataTypeAdapter<Object>[] getDataAdapters() {
    return new IngestWithReducerImpl().getDataAdapters();
  }

  public static class IngestWithReducerImpl implements
      IngestWithReducer<AvroWholeFile, Text, Stanag4676EventWritable, Object>,
      IngestWithMapper<AvroWholeFile, Object> {
    private final SimpleFeatureBuilder ptBuilder;

    private final SimpleFeatureBuilder motionBuilder;

    private final SimpleFeatureBuilder trackBuilder;

    private final SimpleFeatureBuilder missionSummaryBuilder;

    private final SimpleFeatureBuilder missionFrameBuilder;

    private final SimpleFeatureType pointType;

    private final SimpleFeatureType motionPointType;

    private final SimpleFeatureType trackType;

    private final SimpleFeatureType missionSummaryType;

    private final SimpleFeatureType missionFrameType;

    public IngestWithReducerImpl() {
      pointType = Stanag4676Utils.createPointDataType();

      motionPointType = Stanag4676Utils.createMotionDataType();

      trackType = Stanag4676Utils.createTrackDataType();

      missionSummaryType = Stanag4676Utils.createMissionSummaryDataType();

      missionFrameType = Stanag4676Utils.createMissionFrameDataType();

      ptBuilder = new SimpleFeatureBuilder(pointType);

      motionBuilder = new SimpleFeatureBuilder(motionPointType);

      trackBuilder = new SimpleFeatureBuilder(trackType);

      missionSummaryBuilder = new SimpleFeatureBuilder(missionSummaryType);

      missionFrameBuilder = new SimpleFeatureBuilder(missionFrameType);
    }

    @Override
    public String[] getSupportedIndexTypes() {
      return new String[] {SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, TimeField.DEFAULT_FIELD_ID};
    }

    @Override
    public DataTypeAdapter<Object>[] getDataAdapters() {

      return new DataTypeAdapter[] {
          new FeatureDataAdapter(pointType),
          new FeatureDataAdapter(motionPointType),
          new FeatureDataAdapter(trackType),
          new FeatureDataAdapter(missionSummaryType),
          new FeatureDataAdapter(missionFrameType),
          new ImageChipDataAdapter()};
    }

    @Override
    public byte[] toBinary() {
      return new byte[] {};
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public CloseableIterator<KeyValueData<Text, Stanag4676EventWritable>> toIntermediateMapReduceData(
        final AvroWholeFile input) {
      final TrackFileReader fileReader = new TrackFileReader();
      fileReader.setDecoder(new NATO4676Decoder());
      fileReader.setStreaming(true);
      final IngestMessageHandler handler = new IngestMessageHandler();
      fileReader.setHandler(handler);
      fileReader.read(new ByteBufferBackedInputStream(input.getOriginalFile()));
      return new CloseableIterator.Wrapper<>(handler.getIntermediateData().iterator());
    }

    @Override
    public CloseableIterator<GeoWaveData<Object>> toGeoWaveData(
        final Text key,
        final String[] indexNames,
        final Iterable<Stanag4676EventWritable> values) {
      final List<GeoWaveData<Object>> geowaveData = new ArrayList<>();
      // sort events
      final List<Stanag4676EventWritable> sortedEvents = new ArrayList<>();

      for (final Stanag4676EventWritable event : values) {
        sortedEvents.add(Stanag4676EventWritable.clone(event));
      }

      Collections.sort(sortedEvents, new ComparatorStanag4676EventWritable());

      // define event values
      String trackUuid = "";
      String mission = "";
      String trackNumber = "";
      final String trackStatus = "";
      String trackClassification = "";

      // initial values for track point events
      Stanag4676EventWritable firstEvent = null;
      Stanag4676EventWritable lastEvent = null;
      int numTrackPoints = 0;
      double distanceKm = 0.0;
      EarthVector prevEv = null;
      final ArrayList<Double> coord_sequence = new ArrayList<>();
      final ArrayList<Double> detail_coord_sequence = new ArrayList<>();
      double minSpeed = Double.MAX_VALUE;
      double maxSpeed = -Double.MAX_VALUE;

      // initial values for motion events
      int numMotionPoints = 0;
      int stopCount = 0;
      int turnCount = 0;
      int uturnCount = 0;
      int stopDurationContibCount = 0;
      long stopDuration = 0L;
      long stopTime = -1L;

      String objectClass = "";
      String objectClassConf = "";
      String objectClassRel = "";
      String objectClassTimes = "";

      for (final Stanag4676EventWritable event : sortedEvents) {

        trackUuid = event.TrackUUID.toString();
        mission = event.MissionUUID.toString();
        trackNumber = event.TrackNumber.toString();
        trackClassification = event.TrackClassification.toString();

        // build collection of track point
        if (event.EventType.get() == 0) {
          // count number of track points
          numTrackPoints++;

          // grab first and last events
          if (firstEvent == null) {
            firstEvent = event;
          }
          lastEvent = event;

          final EarthVector currentEv =
              new EarthVector(
                  EarthVector.degToRad(event.Latitude.get()),
                  EarthVector.degToRad(event.Longitude.get()),
                  Length.fromM(event.Elevation.get()).getKM());

          if (prevEv != null) {
            distanceKm += prevEv.getDistance(currentEv);
          }

          // populate coordinate sequence
          coord_sequence.add(event.Longitude.get());
          coord_sequence.add(event.Latitude.get());

          prevEv = currentEv;

          final Geometry geometry =
              GeometryUtils.GEOMETRY_FACTORY.createPoint(
                  new Coordinate(event.Longitude.get(), event.Latitude.get()));

          ptBuilder.add(geometry);

          if (!FloatCompareUtils.checkDoublesEqual(
              event.DetailLatitude.get(),
              Stanag4676EventWritable.NO_DETAIL)
              && !FloatCompareUtils.checkDoublesEqual(
                  event.DetailLongitude.get(),
                  Stanag4676EventWritable.NO_DETAIL)) {
            detail_coord_sequence.add(event.DetailLongitude.get());
            detail_coord_sequence.add(event.DetailLatitude.get());
          }

          Double detailLatitude = null;
          Double detailLongitude = null;
          Double detailElevation = null;
          Geometry detailGeometry = null;
          if (!FloatCompareUtils.checkDoublesEqual(
              event.DetailLatitude.get(),
              Stanag4676EventWritable.NO_DETAIL)
              && !FloatCompareUtils.checkDoublesEqual(
                  event.DetailLongitude.get(),
                  Stanag4676EventWritable.NO_DETAIL)) {
            detailLatitude = event.DetailLatitude.get();
            detailLongitude = event.DetailLongitude.get();
            detailElevation = event.DetailElevation.get();
            detailGeometry =
                GeometryUtils.GEOMETRY_FACTORY.createPoint(
                    new Coordinate(detailLongitude, detailLatitude));
          }
          ptBuilder.add(detailGeometry);
          ptBuilder.add(mission);
          ptBuilder.add(trackNumber);
          ptBuilder.add(trackUuid);
          ptBuilder.add(event.TrackItemUUID.toString());
          ptBuilder.add(event.TrackPointSource.toString());
          ptBuilder.add(new Date(event.TimeStamp.get()));
          if (event.Speed.get() > maxSpeed) {
            maxSpeed = event.Speed.get();
          }
          if (event.Speed.get() < minSpeed) {
            minSpeed = event.Speed.get();
          }
          ptBuilder.add(new Double(event.Speed.get()));
          ptBuilder.add(new Double(event.Course.get()));
          // TODO consider more sophisticated tie between track item
          // classification and accumulo visibility
          ptBuilder.add(event.TrackItemClassification.toString());
          ptBuilder.add(new Double(event.Latitude.get()));
          ptBuilder.add(new Double(event.Longitude.get()));
          ptBuilder.add(new Double(event.Elevation.get()));
          ptBuilder.add(detailLatitude);
          ptBuilder.add(detailLongitude);
          ptBuilder.add(detailElevation);
          ptBuilder.add(Integer.valueOf(event.FrameNumber.get()));
          ptBuilder.add(Integer.valueOf(event.PixelRow.get()));
          ptBuilder.add(Integer.valueOf(event.PixelColumn.get()));

          geowaveData.add(
              new GeoWaveData<Object>(
                  Stanag4676Utils.TRACK_POINT,
                  indexNames,
                  ptBuilder.buildFeature(event.TrackItemUUID.toString())));
        }
        // build collection of motion events
        else if (event.EventType.get() == 1) {
          // count number of motion points
          numMotionPoints++;

          motionBuilder.add(
              GeometryUtils.GEOMETRY_FACTORY.createPoint(
                  new Coordinate(event.Longitude.get(), event.Latitude.get())));

          motionBuilder.add(mission);
          motionBuilder.add(trackNumber);
          motionBuilder.add(trackUuid);
          motionBuilder.add(event.TrackItemUUID.toString());
          motionBuilder.add(event.MotionEvent.toString());
          switch (event.MotionEvent.toString()) {
            case "STOP":
              stopCount++;
              stopTime = event.TimeStamp.get();
              break;
            case "START":
              if (stopTime > 0) {
                stopDuration += (event.TimeStamp.get() - stopTime);
                stopDurationContibCount++;
                stopTime = -1L;
              }
              break;
            case "LEFT TURN":
            case "RIGHT TURN":
              turnCount++;
              break;
            case "LEFT U TURN":
            case "RIGHT U TURN":
              uturnCount++;
              break;
            default:
          }
          motionBuilder.add(new Date(event.TimeStamp.get()));
          motionBuilder.add(new Date(event.EndTimeStamp.get()));
          // TODO consider more sophisticated tie between track item
          // classification and accumulo visibility
          motionBuilder.add(event.TrackItemClassification.toString());
          motionBuilder.add(new Double(event.Latitude.get()));
          motionBuilder.add(new Double(event.Longitude.get()));
          motionBuilder.add(new Double(event.Elevation.get()));
          motionBuilder.add(Integer.valueOf(event.FrameNumber.get()));
          motionBuilder.add(Integer.valueOf(event.PixelRow.get()));
          motionBuilder.add(Integer.valueOf(event.PixelColumn.get()));

          geowaveData.add(
              new GeoWaveData<Object>(
                  Stanag4676Utils.MOTION_POINT,
                  indexNames,
                  motionBuilder.buildFeature(event.TrackItemUUID.toString())));
        } else if (event.EventType.get() == 2) {
          final Date date = new Date(event.TimeStamp.get());
          final DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
          format.setTimeZone(TimeZone.getTimeZone("UTC"));
          final String dateStr = format.format(date);

          if (objectClass.length() != 0) {
            objectClass += ",";
          }
          if (objectClassConf.length() != 0) {
            objectClassConf += ",";
          }
          if (objectClassRel.length() != 0) {
            objectClassRel += ",";
          }
          if (objectClassTimes.length() != 0) {
            objectClassTimes += ",";
          }

          objectClass += event.ObjectClass.toString();
          objectClassConf += event.ObjectClassConf.toString();
          objectClassRel += event.ObjectClassRel.toString();
          objectClassTimes += dateStr;
        } else if (event.EventType.get() == 3) {
          missionFrameBuilder.add(
              GeometryUtils.geometryFromBinary(event.Geometry.getBytes(), null));
          missionFrameBuilder.add(event.MissionUUID.toString());
          missionFrameBuilder.add(new Date(event.TimeStamp.get()));
          missionFrameBuilder.add(event.FrameNumber.get());

          geowaveData.add(
              new GeoWaveData<Object>(
                  Stanag4676Utils.MISSION_FRAME,
                  indexNames,
                  missionFrameBuilder.buildFeature(UUID.randomUUID().toString())));
        } else if (event.EventType.get() == 4) {

          missionSummaryBuilder.add(
              GeometryUtils.geometryFromBinary(event.Geometry.getBytes(), null));
          missionSummaryBuilder.add(event.MissionUUID.toString());
          missionSummaryBuilder.add(new Date(event.TimeStamp.get()));
          missionSummaryBuilder.add(new Date(event.EndTimeStamp.get()));
          missionSummaryBuilder.add(event.MissionNumFrames.get());
          missionSummaryBuilder.add(event.MissionName.toString());
          missionSummaryBuilder.add(event.TrackClassification.toString());
          missionSummaryBuilder.add(event.ObjectClass.toString());

          geowaveData.add(
              new GeoWaveData<Object>(
                  Stanag4676Utils.MISSION_SUMMARY,
                  indexNames,
                  missionSummaryBuilder.buildFeature(UUID.randomUUID().toString())));
        }
        if (event.Image != null) {
          final byte[] imageBytes = event.Image.getBytes();
          if ((imageBytes != null) && (imageBytes.length > 0)) {
            geowaveData.add(
                new GeoWaveData(
                    ImageChipDataAdapter.ADAPTER_TYPE_NAME,
                    IMAGE_CHIP_AS_ARRAY,
                    new ImageChip(mission, trackUuid, event.TimeStamp.get(), imageBytes)));
          }
        }
      }

      // create line coordinate sequence
      final Double[] xy = coord_sequence.toArray(new Double[] {});
      if ((firstEvent != null) && (lastEvent != null) && (xy.length >= 4)) {
        final CoordinateSequence2D coordinateSequence =
            new CoordinateSequence2D(ArrayUtils.toPrimitive(xy));
        final LineString lineString =
            GeometryUtils.GEOMETRY_FACTORY.createLineString(coordinateSequence);

        final Double[] dxy = detail_coord_sequence.toArray(new Double[] {});
        final CoordinateSequence2D detailCoordinateSequence =
            new CoordinateSequence2D(ArrayUtils.toPrimitive(dxy));
        LineString detailLineString = null;
        if (detailCoordinateSequence.size() > 0) {
          detailLineString =
              GeometryUtils.GEOMETRY_FACTORY.createLineString(detailCoordinateSequence);
        }
        trackBuilder.add(lineString);
        trackBuilder.add(detailLineString);
        trackBuilder.add(mission);
        trackBuilder.add(trackNumber);
        trackBuilder.add(trackUuid);
        trackBuilder.add(new Date(firstEvent.TimeStamp.get()));
        trackBuilder.add(new Date(lastEvent.TimeStamp.get()));
        final double durationSeconds =
            (lastEvent.TimeStamp.get() - firstEvent.TimeStamp.get()) / 1000.0;
        trackBuilder.add(durationSeconds);
        trackBuilder.add(minSpeed);
        trackBuilder.add(maxSpeed);
        final double distanceM = Length.fromKM(distanceKm).getM();
        final double avgSpeed = durationSeconds > 0 ? distanceM / durationSeconds : 0;
        trackBuilder.add(avgSpeed);
        trackBuilder.add(distanceKm);
        trackBuilder.add(new Double(firstEvent.Latitude.get()));
        trackBuilder.add(new Double(firstEvent.Longitude.get()));
        trackBuilder.add(new Double(lastEvent.Latitude.get()));
        trackBuilder.add(new Double(lastEvent.Longitude.get()));

        Double firstEventDetailLatitude = null;
        Double firstEventDetailLongitude = null;
        Double lastEventDetailLatitude = null;
        Double lastEventDetailLongitude = null;

        if (!FloatCompareUtils.checkDoublesEqual(
            firstEvent.DetailLatitude.get(),
            Stanag4676EventWritable.NO_DETAIL)
            && !FloatCompareUtils.checkDoublesEqual(
                firstEvent.DetailLongitude.get(),
                Stanag4676EventWritable.NO_DETAIL)
            && !FloatCompareUtils.checkDoublesEqual(
                lastEvent.DetailLatitude.get(),
                Stanag4676EventWritable.NO_DETAIL)
            && !FloatCompareUtils.checkDoublesEqual(
                lastEvent.DetailLongitude.get(),
                Stanag4676EventWritable.NO_DETAIL)) {
          firstEventDetailLatitude = firstEvent.DetailLatitude.get();
          firstEventDetailLongitude = firstEvent.DetailLongitude.get();
          lastEventDetailLatitude = lastEvent.DetailLatitude.get();
          lastEventDetailLongitude = lastEvent.DetailLongitude.get();
        }

        trackBuilder.add(firstEventDetailLatitude);
        trackBuilder.add(firstEventDetailLongitude);
        trackBuilder.add(lastEventDetailLatitude);
        trackBuilder.add(lastEventDetailLongitude);

        trackBuilder.add(numTrackPoints);
        trackBuilder.add(numMotionPoints);
        trackBuilder.add(trackStatus);
        trackBuilder.add(turnCount);
        trackBuilder.add(uturnCount);
        trackBuilder.add(stopCount);
        final double stopDurationSeconds = stopDuration / 1000.0;
        trackBuilder.add(stopDurationSeconds);
        trackBuilder.add(
            stopDurationContibCount > 0 ? stopDurationSeconds / stopDurationContibCount : 0.0);
        // TODO consider more sophisticated tie between track
        // classification and accumulo visibility
        trackBuilder.add(trackClassification);

        trackBuilder.add(objectClass);
        trackBuilder.add(objectClassConf);
        trackBuilder.add(objectClassRel);
        trackBuilder.add(objectClassTimes);

        geowaveData.add(
            new GeoWaveData<Object>(
                Stanag4676Utils.TRACK,
                indexNames,
                trackBuilder.buildFeature(trackUuid)));
      }
      return new CloseableIterator.Wrapper<>(geowaveData.iterator());
    }

    @Override
    public CloseableIterator<GeoWaveData<Object>> toGeoWaveData(
        final AvroWholeFile input,
        final String[] indexNames) {
      try (CloseableIterator<KeyValueData<Text, Stanag4676EventWritable>> intermediateData =
          toIntermediateMapReduceData(input)) {
        // this is much better done in the reducer of a map reduce job,
        // this aggregation by track UUID is not memory efficient
        final Map<Text, List<Stanag4676EventWritable>> trackUuidMap = new HashMap<>();
        while (intermediateData.hasNext()) {
          final KeyValueData<Text, Stanag4676EventWritable> next = intermediateData.next();
          List<Stanag4676EventWritable> trackEvents = trackUuidMap.get(next.getKey());
          if (trackEvents == null) {
            trackEvents = new ArrayList<>();
            trackUuidMap.put(next.getKey(), trackEvents);
          }
          trackEvents.add(next.getValue());
        }
        final List<CloseableIterator<GeoWaveData<Object>>> iterators = new ArrayList<>();
        for (final Entry<Text, List<Stanag4676EventWritable>> entry : trackUuidMap.entrySet()) {
          iterators.add(toGeoWaveData(entry.getKey(), indexNames, entry.getValue()));
        }
        return new CloseableIterator.Wrapper<>(Iterators.concat(iterators.iterator()));
      }
    }
  }

  @Override
  public Index[] getRequiredIndices() {
    return new Index[] {IMAGE_CHIP_INDEX};
  }

  @Override
  public IngestPluginBase<AvroWholeFile, Object> getIngestWithAvroPlugin() {
    return ingestWithMapper();
  }

  @Override
  public String[] getSupportedIndexTypes() {
    return new String[] {SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, TimeField.DEFAULT_FIELD_ID};
  }

}
