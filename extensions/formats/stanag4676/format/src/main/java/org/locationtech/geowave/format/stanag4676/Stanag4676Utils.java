/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676;

import java.util.Date;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors.TimeDescriptorConfiguration;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.simple.SimpleFeatureType;

public class Stanag4676Utils {
  public static final String TRACK_POINT = "track_point";
  public static final String MOTION_POINT = "motion_point";
  public static final String TRACK = "track";
  public static final String MISSION_SUMMARY = "mission_summary";
  public static final String MISSION_FRAME = "mission_frame";
  public static final String NAMESPACE = "http://github.com/locationtech/geowave";

  public static SimpleFeatureType createPointDataType() {

    final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
    simpleFeatureTypeBuilder.setName(TRACK_POINT);
    simpleFeatureTypeBuilder.setNamespaceURI(NAMESPACE);

    final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Point.class).buildDescriptor("geometry"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Point.class).buildDescriptor("DetailGeometry"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("Mission"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("TrackNumber"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("TrackUUID"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("TrackItemUUID"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("TrackPointSource"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Date.class).buildDescriptor("TimeStamp"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("Speed"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("Course"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("Classification"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("Latitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("Longitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("Elevation"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("DetailLatitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("DetailLongitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("DetailElevation"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("FrameNumber"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("PixelRow"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("PixelColumn"));

    simpleFeatureTypeBuilder.setDefaultGeometry("geometry");

    final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
    timeConfig.setTimeName("TimeStamp");
    final SimpleFeatureType type = simpleFeatureTypeBuilder.buildFeatureType();
    timeConfig.updateType(type);
    return type;
  }

  public static SimpleFeatureType createMotionDataType() {

    final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
    simpleFeatureTypeBuilder.setName(MOTION_POINT);
    simpleFeatureTypeBuilder.setNamespaceURI(NAMESPACE);

    final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Point.class).buildDescriptor("geometry"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("Mission"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("TrackNumber"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("TrackUUID"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("TrackItemUUID"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("MotionEvent"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Date.class).buildDescriptor("StartTime"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Date.class).buildDescriptor("EndTime"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("Classification"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("Latitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("Longitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("Elevation"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("FrameNumber"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("PixelRow"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("PixelColumn"));

    final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
    timeConfig.setStartRangeName("StartTime");
    timeConfig.setEndRangeName("EndTime");
    final SimpleFeatureType type = simpleFeatureTypeBuilder.buildFeatureType();
    timeConfig.updateType(type);
    return type;
  }

  public static SimpleFeatureType createTrackDataType() {

    final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
    simpleFeatureTypeBuilder.setName(TRACK);
    simpleFeatureTypeBuilder.setNamespaceURI(NAMESPACE);

    final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(LineString.class).buildDescriptor("geometry"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(LineString.class).buildDescriptor("DetailGeometry"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("Mission"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("TrackNumber"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("TrackUUID"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Date.class).buildDescriptor("StartTime"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Date.class).buildDescriptor("EndTime"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("Duration"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("MinSpeed"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("MaxSpeed"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("AvgSpeed"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("Distance"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("StartLatitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("StartLongitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("EndLatitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("EndLongitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("DetailStartLatitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("DetailStartLongitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("DetailEndLatitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("DetailEndLongitude"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("PointCount"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("EventCount"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("TrackStatus"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("TurnCount"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("UTurnCount"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("StopCount"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("StopDuration"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Double.class).buildDescriptor("AvgStopDuration"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("Classification"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("ObjectClass"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("ObjectClassConf"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("ObjectClassRel"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("ObjectClassTime"));

    simpleFeatureTypeBuilder.setDefaultGeometry("geometry");

    final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
    timeConfig.setStartRangeName("StartTime");
    timeConfig.setEndRangeName("EndTime");
    final SimpleFeatureType type = simpleFeatureTypeBuilder.buildFeatureType();
    timeConfig.updateType(type);
    return type;
  }

  public static SimpleFeatureType createMissionSummaryDataType() {

    final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
    simpleFeatureTypeBuilder.setName(MISSION_SUMMARY);
    simpleFeatureTypeBuilder.setNamespaceURI(NAMESPACE);

    final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Polygon.class).buildDescriptor("geometry"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("Mission"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Date.class).buildDescriptor("StartTime"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Date.class).buildDescriptor("EndTime"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("NumberOfFrames"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("Name"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("Security"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("ActiveObjectClass"));

    final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
    timeConfig.setStartRangeName("StartTime");
    timeConfig.setEndRangeName("EndTime");
    final SimpleFeatureType type = simpleFeatureTypeBuilder.buildFeatureType();
    timeConfig.updateType(type);
    return type;
  }

  public static SimpleFeatureType createMissionFrameDataType() {

    final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
    simpleFeatureTypeBuilder.setName(MISSION_FRAME);
    simpleFeatureTypeBuilder.setNamespaceURI(NAMESPACE);

    final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Polygon.class).buildDescriptor("geometry"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(String.class).buildDescriptor("Mission"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Date.class).buildDescriptor("TimeStamp"));
    simpleFeatureTypeBuilder.add(
        attributeTypeBuilder.binding(Integer.class).buildDescriptor("FrameNumber"));

    final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
    timeConfig.setTimeName("TimeStamp");
    final SimpleFeatureType type = simpleFeatureTypeBuilder.buildFeatureType();
    timeConfig.updateType(type);
    return type;
  }
}
