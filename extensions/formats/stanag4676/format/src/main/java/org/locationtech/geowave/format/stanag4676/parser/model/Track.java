/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676.parser.model;

import java.util.ArrayList;
import java.util.List;

public class Track {
  private Long id;

  private String uuid;
  private String trackNumber;

  private TrackStatus status;
  private Security security;
  private String comment;
  private List<TrackPoint> points = new ArrayList<>();
  private List<TrackIdentity> identities = new ArrayList<>();
  private List<TrackClassification> classifications = new ArrayList<>();
  private List<TrackManagement> managements = new ArrayList<>();
  private List<MotionImagery> motionImages;
  private List<LineageRelation> trackRelations = new ArrayList<>();

  public Long getId() {
    return id;
  }

  public void setId(final Long id) {
    this.id = id;
  }

  /**
   * The UUID of a track
   *
   * @return UUID
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * Sets the UUID of the track
   *
   * @param uuid
   */
  public void setUuid(final String uuid) {
    this.uuid = uuid;
  }

  public String getTrackNumber() {
    return trackNumber;
  }

  public void setTrackNumber(final String trackNumber) {
    this.trackNumber = trackNumber;
  }

  public TrackStatus getStatus() {
    return status;
  }

  public void setStatus(final TrackStatus status) {
    this.status = status;
  }

  public Security getSecurity() {
    return security;
  }

  public void setSecurity(final Security security) {
    this.security = security;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(final String comment) {
    this.comment = comment;
  }

  /**
   * A list of the TrackPoints which comprise this track
   *
   * @return A list of the TrackPoints which comprise this track
   */
  public List<TrackPoint> getPoints() {
    return points;
  }

  /**
   * Sets the list of TrackPoints which comprise this track
   *
   * @param points the list of TrackPoints which comprise this track
   */
  public void setPoints(final List<TrackPoint> points) {
    this.points = points;
  }

  /**
   * Adds a TrackPoint to the list of TrackPoints comprise this track
   *
   * @param point the TrackPoint to add
   */
  public void addPoint(final TrackPoint point) {
    if (points == null) {
      points = new ArrayList<>();
    }
    points.add(point);
  }

  /**
   * Provides identity information about a track.
   *
   * <p> values are derived from STANAG 1241.
   *
   * @return {@link TrackIdentity}
   */
  public List<TrackIdentity> getIdentities() {
    return identities;
  }

  public void setIdentities(final List<TrackIdentity> identities) {
    this.identities = identities;
  }

  /**
   * sets the identity information about this track
   *
   * @param identity {@link TrackIdentity}
   */
  public void addIdentity(final TrackIdentity identity) {
    if (identities == null) {
      identities = new ArrayList<>();
    }
    identities.add(identity);
  }

  /**
   * Provides classification information about this track
   *
   * @return {@link TrackClassification}
   */
  public List<TrackClassification> getClassifications() {
    return classifications;
  }

  public void setClassifications(final List<TrackClassification> classifications) {
    this.classifications = classifications;
  }

  /**
   * sets the classification information about this track
   *
   * @param classification {@link TrackClassification}
   */
  public void addClassification(final TrackClassification classification) {
    if (classifications == null) {
      classifications = new ArrayList<>();
    }
    classifications.add(classification);
  }

  /**
   * Provides management information about this track
   *
   * @return {@link TrackManagement}
   */
  public List<TrackManagement> getManagements() {
    return managements;
  }

  public void setManagements(final List<TrackManagement> managements) {
    this.managements = managements;
  }

  /**
   * sets the management information about this track
   *
   * @param management {@link TrackManagement}
   */
  public void addManagement(final TrackManagement management) {
    if (managements == null) {
      managements = new ArrayList<>();
    }
    managements.add(management);
  }

  /**
   * Provides a list of related tracks
   *
   * @return List<{@link LineageRelation}>
   */
  public List<LineageRelation> getTrackRelations() {
    return trackRelations;
  }

  public void setTrackRelations(final List<LineageRelation> trackRelations) {
    this.trackRelations = trackRelations;
  }

  /**
   * Adds a track relation
   */
  public void addTrackRelation(final LineageRelation relation) {
    if (trackRelations == null) {
      trackRelations = new ArrayList<>();
    }
    trackRelations.add(relation);
  }

  /**
   * Provides video (motion imagery) information about this track
   *
   * @return {@link MotionImagery}
   */
  public List<MotionImagery> getMotionImages() {
    return motionImages;
  }

  public void setMotionImages(final List<MotionImagery> motionImages) {
    this.motionImages = motionImages;
  }

  public void addMotionImagery(final MotionImagery image) {
    if (motionImages == null) {
      motionImages = new ArrayList<>();
    }
    motionImages.add(image);
  }
}
