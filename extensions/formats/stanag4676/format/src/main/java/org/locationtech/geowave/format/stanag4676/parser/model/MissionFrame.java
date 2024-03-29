/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676.parser.model;

public class MissionFrame {
  private Area coverageArea;
  private long frameTime;
  private String missionId;
  private Integer frameNumber;

  /** @return the missionId */
  public String getMissionId() {
    return missionId;
  }

  /** @param missionId the missionId to set */
  public void setMissionId(final String missionId) {
    this.missionId = missionId;
  }

  /** @return the frameNumber */
  public Integer getFrameNumber() {
    return frameNumber;
  }

  /** @param frameNumber the frameNumber to set */
  public void setFrameNumber(final Integer frameNumber) {
    this.frameNumber = frameNumber;
  }

  /** @return the frameTime */
  public long getFrameTime() {
    return frameTime;
  }

  /** @param frameTime the frameTime to set */
  public void setFrameTime(final long frameTime) {
    this.frameTime = frameTime;
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
