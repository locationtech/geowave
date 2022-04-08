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

public class MissionSummary {
  private Area coverageArea;
  private String missionId;
  private String name;
  private String security;
  private long startTime;
  private long endTime;

  private List<MissionFrame> frames = new ArrayList<>();
  private List<ObjectClassification> classifications = new ArrayList<>();

  /** @return the missionId */
  public String getMissionId() {
    return missionId;
  }

  /** @param missionId the missionId to set */
  public void setMissionId(final String missionId) {
    this.missionId = missionId;
  }

  /**
   * The name of a mission
   *
   * @return name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the name of the mission
   *
   * @param name
   */
  public void setName(final String name) {
    this.name = name;
  }

  /**
   * The security of a mission
   *
   * @return security
   */
  public String getSecurity() {
    return security;
  }

  /**
   * Sets the security of the mission
   *
   * @param security
   */
  public void setSecurity(final String security) {
    this.security = security;
  }

  /**
   * A list of the frames which comprise this mission
   *
   * @return A list of the frames which comprise this mission
   */
  public List<MissionFrame> getFrames() {
    return frames;
  }

  /**
   * Sets the list of frames which comprise this mission
   *
   * @param frames the list of frames which comprise this mission
   */
  public void setFrames(final List<MissionFrame> frames) {
    this.frames = frames;
  }

  /**
   * Adds a MissionFrame
   *
   * @param frame the MissionFrame to add
   */
  public void addFrame(final MissionFrame frame) {
    if (frames == null) {
      frames = new ArrayList<>();
    }
    frames.add(frame);
  }

  /**
   * Provides object classification information about this mission
   *
   * @return {@link ObjectClassification}
   */
  public List<ObjectClassification> getClassifications() {
    return classifications;
  }

  public void setClassifications(final List<ObjectClassification> classifications) {
    this.classifications = classifications;
  }

  /**
   * sets the object classification information about this mission
   *
   * @param classification {@link ObjectClassification}
   */
  public void addClassification(final ObjectClassification classification) {
    if (classifications == null) {
      classifications = new ArrayList<>();
    }
    classifications.add(classification);
  }

  /** @return the startTime */
  public long getStartTime() {
    return startTime;
  }

  /** @param startTime the startTime to set */
  public void setStartTime(final long startTime) {
    this.startTime = startTime;
  }

  /** @return the endTime */
  public long getEndTime() {
    return endTime;
  }

  /** @param endTime the endTime to set */
  public void setEndTime(final long endTime) {
    this.endTime = endTime;
  }

  /** @return the coverageArea */
  public Area getCoverageArea() {
    return coverageArea;
  }

  /** @param coverageArea the coverageArea to set */
  public void setCoverageArea(final Area coverageArea) {
    this.coverageArea = coverageArea;
  }
}
