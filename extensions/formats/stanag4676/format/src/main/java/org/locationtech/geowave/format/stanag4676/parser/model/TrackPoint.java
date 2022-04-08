/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676.parser.model;

import java.util.List;

public class TrackPoint {
  private Long id;
  /** UUID for this TrackPoint */
  public String uuid;

  public Security security;

  /**
   * Date and Time of this event (track item).
   *
   * <p> for example, indicating the time when the tracked target was on a specific position. Values
   * and formats In accordance with the W3C recommendation for implementation of ISO 8601 standard.
   */
  public long eventTime;

  public String trackItemSource;

  public String trackItemComment;
  /** The position of an object being tracked. */
  public GeodeticPosition location;

  /** The speed of an object being tracked, expressed in meters per second (m/s). */
  public Double speed = 0.0;

  /**
   * The course of an object being tracked, expressed in decimal degrees and measured from true
   * north in a clockwise direction
   */
  public Double course = 0.0;

  /** The motion event */
  public String motionEvent = "";

  /**
   * Information of whether a track point is estimated, or predicted manually or automatically. See
   * {@link TrackPointType}
   */
  public TrackPointType trackPointType;

  /** information related to the source of the track point data. (i.e. radar, video, ESM). */
  public ModalityType trackPointSource;

  /**
   * A spatial outline of an object being tracked.
   *
   * <p> for example, in case of video tracking, a box or polygon surrounding the object may be
   * specified.
   */
  public Area objectMask;

  public TrackPointDetail detail;

  private List<TrackDotSource> dotSources;

  public Long getId() {
    return id;
  }

  public void setId(final Long id) {
    this.id = id;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(final String uuid) {
    this.uuid = uuid;
  }

  public Security getSecurity() {
    return security;
  }

  public void setSecurity(final Security security) {
    this.security = security;
  }

  public long getEventTime() {
    return eventTime;
  }

  public void setEventTime(final long eventTime) {
    this.eventTime = eventTime;
  }

  public String getTrackItemSource() {
    return trackItemSource;
  }

  public void setTrackItemSource(final String trackItemSource) {
    this.trackItemSource = trackItemSource;
  }

  public String getTrackItemComment() {
    return trackItemComment;
  }

  public void setTrackItemComment(final String trackItemComment) {
    this.trackItemComment = trackItemComment;
  }

  public GeodeticPosition getLocation() {
    return location;
  }

  public void setLocation(final GeodeticPosition location) {
    this.location = location;
  }

  public Double getSpeed() {
    return speed;
  }

  public void setSpeed(final Double speed) {
    this.speed = speed;
  }

  public Double getCourse() {
    return course;
  }

  public void setCourse(final Double course) {
    this.course = course;
  }

  public TrackPointType getTrackPointType() {
    return trackPointType;
  }

  public void setTrackPointType(final TrackPointType trackPointType) {
    this.trackPointType = trackPointType;
  }

  public ModalityType getTrackPointSource() {
    return trackPointSource;
  }

  public void setTrackPointSource(final ModalityType trackPointSource) {
    this.trackPointSource = trackPointSource;
  }

  public Area getObjectMask() {
    return objectMask;
  }

  public void setObjectMask(final Area objectMask) {
    this.objectMask = objectMask;
  }

  public TrackPointDetail getDetail() {
    return detail;
  }

  public void setDetail(final TrackPointDetail detail) {
    this.detail = detail;
  }

  public List<TrackDotSource> getDotSources() {
    return dotSources;
  }

  public void setDotSources(final List<TrackDotSource> dotSources) {
    this.dotSources = dotSources;
  }
}
