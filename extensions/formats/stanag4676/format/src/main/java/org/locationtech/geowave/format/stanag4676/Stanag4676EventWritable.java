/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Stanag4676EventWritable implements Writable {
  public static final double NO_DETAIL = Double.MIN_VALUE;

  // 0 = point event
  // 1 = motion event
  // 2 = track object classification event
  // 3 = mission frame event
  // 4 = mission summary event
  public IntWritable EventType;
  public BytesWritable Geometry;
  public BytesWritable DetailGeometry;
  public BytesWritable Image;
  public Text MissionUUID;
  public Text MissionName;
  public IntWritable MissionNumFrames;
  public Text TrackNumber;
  public Text TrackUUID;
  public Text TrackStatus;
  public Text TrackClassification;
  public Text TrackItemUUID;
  public Text TrackPointSource;
  public LongWritable TimeStamp;
  public LongWritable EndTimeStamp;
  public DoubleWritable Speed;
  public DoubleWritable Course;
  public Text TrackItemClassification;
  public DoubleWritable Latitude;
  public DoubleWritable Longitude;
  public DoubleWritable Elevation;
  public DoubleWritable DetailLatitude;
  public DoubleWritable DetailLongitude;
  public DoubleWritable DetailElevation;
  public IntWritable PixelRow;
  public IntWritable PixelColumn;
  public Text MotionEvent;
  public IntWritable FrameNumber;
  public Text ObjectClass;
  public IntWritable ObjectClassConf;
  public IntWritable ObjectClassRel;

  public static Stanag4676EventWritable clone(final Stanag4676EventWritable sw) {
    final Stanag4676EventWritable sw2 = new Stanag4676EventWritable();
    sw2.setEventType(new IntWritable(sw.getEventType().get()));
    sw2.setGeometry(new BytesWritable(sw.getGeometry().copyBytes()));
    sw2.setDetailGeometry(new BytesWritable(sw.getDetailGeometry().copyBytes()));
    sw2.setImage(new BytesWritable(sw.getImage().copyBytes()));
    sw2.setMissionUUID(new Text(sw.getMissionUUID().toString()));
    sw2.setMissionName(new Text(sw.getMissionName().toString()));
    sw2.setMissionNumFrames(new IntWritable(sw.getMissionNumFrames().get()));
    sw2.setTrackNumber(new Text(sw.getTrackNumber().toString()));
    sw2.setTrackUUID(new Text(sw.getTrackUUID().toString()));
    sw2.setTrackStatus(new Text(sw.getTrackStatus().toString()));
    sw2.setTrackClassification(new Text(sw.getTrackClassification().toString()));
    sw2.setTrackItemUUID(new Text(sw.getTrackItemUUID().toString()));
    sw2.setTrackPointSource(new Text(sw.getTrackPointSource().toString()));
    sw2.setTimeStamp(new LongWritable(sw.getTimeStamp().get()));
    sw2.setEndTimeStamp(new LongWritable(sw.getEndTimeStamp().get()));
    sw2.setSpeed(new DoubleWritable(sw.getSpeed().get()));
    sw2.setCourse(new DoubleWritable(sw.getCourse().get()));
    sw2.setTrackItemClassification(new Text(sw.getTrackItemClassification().toString()));
    sw2.setLatitude(new DoubleWritable(sw.getLatitude().get()));
    sw2.setLongitude(new DoubleWritable(sw.getLongitude().get()));
    sw2.setElevation(new DoubleWritable(sw.getElevation().get()));
    sw2.setDetailLatitude(new DoubleWritable(sw.getDetailLatitude().get()));
    sw2.setDetailLongitude(new DoubleWritable(sw.getDetailLongitude().get()));
    sw2.setDetailElevation(new DoubleWritable(sw.getDetailElevation().get()));
    sw2.setPixelRow(new IntWritable(sw.getPixelRow().get()));
    sw2.setPixelColumn(new IntWritable(sw.getPixelColumn().get()));
    sw2.setMotionEvent(new Text(sw.getMotionEvent().toString()));
    sw2.setFrameNumber(new IntWritable(sw.getFrameNumber().get()));
    sw2.setObjectClass(new Text(sw.getObjectClass().toString()));
    sw2.setObjectClassConf(new IntWritable(sw.getObjectClassConf().get()));
    sw2.setObjectClassRel(new IntWritable(sw.getObjectClassRel().get()));

    return sw2;
  }

  public Stanag4676EventWritable() {
    EventType = new IntWritable();
    Geometry = new BytesWritable();
    DetailGeometry = new BytesWritable();
    Image = new BytesWritable();
    MissionUUID = new Text();
    MissionName = new Text();
    MissionNumFrames = new IntWritable();
    TrackNumber = new Text();
    TrackUUID = new Text();
    TrackStatus = new Text();
    TrackClassification = new Text();
    TrackItemUUID = new Text();
    TrackPointSource = new Text();
    TimeStamp = new LongWritable();
    EndTimeStamp = new LongWritable();
    Speed = new DoubleWritable();
    Course = new DoubleWritable();
    TrackItemClassification = new Text();
    Latitude = new DoubleWritable();
    Longitude = new DoubleWritable();
    Elevation = new DoubleWritable();
    DetailLatitude = new DoubleWritable();
    DetailLongitude = new DoubleWritable();
    DetailElevation = new DoubleWritable();
    PixelRow = new IntWritable();
    PixelColumn = new IntWritable();
    MotionEvent = new Text();
    FrameNumber = new IntWritable();
    ObjectClass = new Text();
    ObjectClassConf = new IntWritable();
    ObjectClassRel = new IntWritable();
  }

  public void setTrackPointData(
      final byte[] geometry,
      final byte[] detailGeometry,
      final byte[] image,
      final String missionUUID,
      final String trackNumber,
      final String trackUUID,
      final String trackStatus,
      final String trackClassification,
      final String trackItemUUID,
      final String trackPointSource,
      final long timeStamp,
      final long endTimeStamp,
      final double speed,
      final double course,
      final String trackItemClassification,
      final double latitude,
      final double longitude,
      final double elevation,
      final double detailLatitude,
      final double detailLongitude,
      final double detailElevation,
      final int pixelRow,
      final int pixelColumn,
      final int frameNumber) {
    EventType = new IntWritable(0);
    Geometry = new BytesWritable(geometry);
    if (detailGeometry != null) {
      DetailGeometry = new BytesWritable(detailGeometry);
    }
    if (image != null) {
      Image = new BytesWritable(image);
    }
    MissionUUID = new Text(missionUUID);
    TrackNumber = new Text(trackNumber);
    TrackUUID = new Text(trackUUID);
    TrackStatus = new Text(trackStatus);
    TrackClassification = new Text(trackClassification);
    TrackItemUUID = new Text(trackItemUUID);
    TrackPointSource = new Text(trackPointSource);
    TimeStamp = new LongWritable(timeStamp);
    EndTimeStamp = new LongWritable(endTimeStamp);
    Speed = new DoubleWritable(speed);
    Course = new DoubleWritable(course);
    TrackItemClassification = new Text(trackItemClassification);
    Latitude = new DoubleWritable(latitude);
    Longitude = new DoubleWritable(longitude);
    Elevation = new DoubleWritable(elevation);
    DetailLatitude = new DoubleWritable(detailLatitude);
    DetailLongitude = new DoubleWritable(detailLongitude);
    DetailElevation = new DoubleWritable(detailElevation);
    PixelRow = new IntWritable(pixelRow);
    PixelColumn = new IntWritable(pixelColumn);
    FrameNumber = new IntWritable(frameNumber);
  }

  public void setMotionPointData(
      final byte[] geometry,
      final byte[] image,
      final String missionUUID,
      final String trackNumber,
      final String trackUUID,
      final String trackStatus,
      final String trackClassification,
      final String trackItemUUID,
      final String trackPointSource,
      final long timeStamp,
      final long endTimeStamp,
      final double speed,
      final double course,
      final String trackItemClassification,
      final double latitude,
      final double longitude,
      final double elevation,
      final int pixelRow,
      final int pixelColumn,
      final int frameNumber,
      final String motionEvent) {
    EventType = new IntWritable(1);
    Geometry = new BytesWritable(geometry);
    if (image != null) {
      Image = new BytesWritable(image);
    }

    MissionUUID = new Text(missionUUID);
    TrackNumber = new Text(trackNumber);
    TrackUUID = new Text(trackUUID);
    TrackStatus = new Text(trackStatus);
    TrackClassification = new Text(trackClassification);
    TrackItemUUID = new Text(trackItemUUID);
    TrackPointSource = new Text(trackPointSource);
    TimeStamp = new LongWritable(timeStamp);
    EndTimeStamp = new LongWritable(endTimeStamp);
    Speed = new DoubleWritable(speed);
    Course = new DoubleWritable(course);
    TrackItemClassification = new Text(trackItemClassification);
    Latitude = new DoubleWritable(latitude);
    Longitude = new DoubleWritable(longitude);
    Elevation = new DoubleWritable(elevation);
    PixelRow = new IntWritable(pixelRow);
    PixelColumn = new IntWritable(pixelColumn);
    FrameNumber = new IntWritable(frameNumber);
    MotionEvent = new Text(motionEvent);
  }

  public void setTrackObjectClassData(
      final long timeStamp,
      final String objectClass,
      final int objectConf,
      final int objectRel) {
    EventType = new IntWritable(2);
    TimeStamp = new LongWritable(timeStamp);
    ObjectClass = new Text(objectClass);
    ObjectClassConf = new IntWritable(objectConf);
    ObjectClassRel = new IntWritable(objectRel);
  }

  public void setMissionFrameData(
      final byte[] geometry,
      final String missionUUID,
      final int number,
      final long timeStamp) {
    EventType = new IntWritable(3);
    Geometry = new BytesWritable(geometry);
    MissionUUID = new Text(missionUUID);
    FrameNumber = new IntWritable(number);
    TimeStamp = new LongWritable(timeStamp);
  }

  public void setMissionSummaryData(
      final byte[] geometry,
      final String missionUUID,
      final String missionName,
      final int missionNumFrames,
      final long timeStamp,
      final long endTimeStamp,
      final String classification,
      final String objectClass) {
    EventType = new IntWritable(4);
    Geometry = new BytesWritable(geometry);
    MissionUUID = new Text(missionUUID);
    MissionName = new Text(missionName);
    MissionNumFrames = new IntWritable(missionNumFrames);
    TimeStamp = new LongWritable(timeStamp);
    EndTimeStamp = new LongWritable(endTimeStamp);
    TrackClassification = new Text(classification);
    ObjectClass = new Text(objectClass);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    EventType.readFields(in);
    Geometry.readFields(in);
    DetailGeometry.readFields(in);
    Image.readFields(in);
    MissionUUID.readFields(in);
    MissionName.readFields(in);
    MissionNumFrames.readFields(in);
    TrackNumber.readFields(in);
    TrackUUID.readFields(in);
    TrackStatus.readFields(in);
    TrackClassification.readFields(in);
    TrackItemUUID.readFields(in);
    TrackPointSource.readFields(in);
    TimeStamp.readFields(in);
    EndTimeStamp.readFields(in);
    Speed.readFields(in);
    Course.readFields(in);
    TrackItemClassification.readFields(in);
    Latitude.readFields(in);
    Longitude.readFields(in);
    Elevation.readFields(in);
    DetailLatitude.readFields(in);
    DetailLongitude.readFields(in);
    DetailElevation.readFields(in);
    PixelRow.readFields(in);
    PixelColumn.readFields(in);
    FrameNumber.readFields(in);
    MotionEvent.readFields(in);
    ObjectClass.readFields(in);
    ObjectClassConf.readFields(in);
    ObjectClassRel.readFields(in);
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    EventType.write(out);
    Geometry.write(out);
    DetailGeometry.write(out);
    Image.write(out);
    MissionUUID.write(out);
    MissionName.write(out);
    MissionNumFrames.write(out);
    TrackNumber.write(out);
    TrackUUID.write(out);
    TrackStatus.write(out);
    TrackClassification.write(out);
    TrackItemUUID.write(out);
    TrackPointSource.write(out);
    TimeStamp.write(out);
    EndTimeStamp.write(out);
    Speed.write(out);
    Course.write(out);
    TrackItemClassification.write(out);
    Latitude.write(out);
    Longitude.write(out);
    Elevation.write(out);
    DetailLatitude.write(out);
    DetailLongitude.write(out);
    DetailElevation.write(out);
    PixelRow.write(out);
    PixelColumn.write(out);
    FrameNumber.write(out);
    MotionEvent.write(out);
    ObjectClass.write(out);
    ObjectClassConf.write(out);
    ObjectClassRel.write(out);
  }

  // Getter and setter methods for all fields
  public IntWritable getEventType() {
    return EventType;
  }

  public void setEventType(IntWritable eventType) {
    this.EventType = eventType;
  }

  public BytesWritable getGeometry() {
    return Geometry;
  }

  public void setGeometry(BytesWritable geometry) {
    this.Geometry = geometry;
  }

  public BytesWritable getDetailGeometry() {
    return DetailGeometry;
  }

  public void setDetailGeometry(BytesWritable detailGeometry) {
    this.DetailGeometry = detailGeometry;
  }

  public BytesWritable getImage() {
    return Image;
  }

  public void setImage(BytesWritable image) {
    this.Image = image;
  }

  public Text getMissionUUID() {
    return MissionUUID;
  }

  public void setMissionUUID(Text missionUUID) {
    this.MissionUUID = missionUUID;
  }

  public Text getMissionName() {
    return MissionName;
  }

  public void setMissionName(Text missionName) {
    this.MissionName = missionName;
  }

  public IntWritable getMissionNumFrames() {
    return MissionNumFrames;
  }

  public void setMissionNumFrames(IntWritable missionNumFrames) {
    this.MissionNumFrames = missionNumFrames;
  }

  public Text getTrackNumber() {
    return TrackNumber;
  }

  public void setTrackNumber(Text trackNumber) {
    this.TrackNumber = trackNumber;
  }

  public Text getTrackUUID() {
    return TrackUUID;
  }

  public void setTrackUUID(Text trackUUID) {
    this.TrackUUID = trackUUID;
  }

  public Text getTrackStatus() {
    return TrackStatus;
  }

  public void setTrackStatus(Text trackStatus) {
    this.TrackStatus = trackStatus;
  }

  public Text getTrackClassification() {
    return TrackClassification;
  }

  public void setTrackClassification(Text trackClassification) {
    this.TrackClassification = trackClassification;
  }

  public Text getTrackItemUUID() {
    return TrackItemUUID;
  }

  public void setTrackItemUUID(Text trackItemUUID) {
    this.TrackItemUUID = trackItemUUID;
  }

  public Text getTrackPointSource() {
    return TrackPointSource;
  }

  public void setTrackPointSource(Text trackPointSource) {
    this.TrackPointSource = trackPointSource;
  }

  public LongWritable getTimeStamp() {
    return TimeStamp;
  }

  public void setTimeStamp(LongWritable timeStamp) {
    this.TimeStamp = timeStamp;
  }

  public LongWritable getEndTimeStamp() {
    return EndTimeStamp;
  }

  public void setEndTimeStamp(LongWritable endTimeStamp) {
    this.EndTimeStamp = endTimeStamp;
  }

  public DoubleWritable getSpeed() {
    return Speed;
  }

  public void setSpeed(DoubleWritable speed) {
    this.Speed = speed;
  }

  public DoubleWritable getCourse() {
    return Course;
  }

  public void setCourse(DoubleWritable course) {
    this.Course = course;
  }

  public Text getTrackItemClassification() {
    return TrackItemClassification;
  }

  public void setTrackItemClassification(Text trackItemClassification) {
    this.TrackItemClassification = trackItemClassification;
  }

  public DoubleWritable getLatitude() {
    return Latitude;
  }

  public void setLatitude(DoubleWritable latitude) {
    this.Latitude = latitude;
  }

  public DoubleWritable getLongitude() {
    return Longitude;
  }

  public void setLongitude(DoubleWritable longitude) {
    this.Longitude = longitude;
  }

  public DoubleWritable getElevation() {
    return Elevation;
  }

  public void setElevation(DoubleWritable elevation) {
    this.Elevation = elevation;
  }

  public DoubleWritable getDetailLatitude() {
    return DetailLatitude;
  }

  public void setDetailLatitude(DoubleWritable detailLatitude) {
    this.DetailLatitude = detailLatitude;
  }

  public DoubleWritable getDetailLongitude() {
    return DetailLongitude;
  }

  public void setDetailLongitude(DoubleWritable detailLongitude) {
    this.DetailLongitude = detailLongitude;
  }

  public DoubleWritable getDetailElevation() {
    return DetailElevation;
  }

  public void setDetailElevation(DoubleWritable detailElevation) {
    this.DetailElevation = detailElevation;
  }

  public IntWritable getPixelRow() {
    return PixelRow;
  }

  public void setPixelRow(IntWritable pixelRow) {
    this.PixelRow = pixelRow;
  }

  public IntWritable getPixelColumn() {
    return PixelColumn;
  }

  public void setPixelColumn(IntWritable pixelColumn) {
    this.PixelColumn = pixelColumn;
  }

  public Text getMotionEvent() {
    return MotionEvent;
  }

  public void setMotionEvent(Text motionEvent) {
    this.MotionEvent = motionEvent;
  }

  public IntWritable getFrameNumber() {
    return FrameNumber;
  }

  public void setFrameNumber(IntWritable frameNumber) {
    this.FrameNumber = frameNumber;
  }

  public Text getObjectClass() {
    return ObjectClass;
  }

  public void setObjectClass(Text objectClass) {
    this.ObjectClass = objectClass;
  }

  public IntWritable getObjectClassConf() {
    return ObjectClassConf;
  }

  public void setObjectClassConf(IntWritable objectClassConf) {
    this.ObjectClassConf = objectClassConf;
  }

  public IntWritable getObjectClassRel() {
    return ObjectClassRel;
  }

  public void setObjectClassRel(IntWritable objectClassRel) {
    this.ObjectClassRel = objectClassRel;
  }
}
