/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676.parser.model;

public class TrackPointDetail {

  public GeodeticPosition location;
  /** The X component of the track velocity */
  public Double velocityX;

  /** The Y component of the track velocity */
  public Double velocityY;

  /** The Z component of the track velocity */
  public Double velocityZ;

  /** The X component of the track acceleration */
  public Double accelerationX;

  /** The Y component of the track acceleration */
  public Double accelerationY;

  /** The Z component of the track acceleration */
  public Double accelerationZ;

  /** The covariance matrix related to the state vector associated with a reported track point. */
  public CovarianceMatrix covarianceMatrix;

  public GeodeticPosition getLocation() {
    return location;
  }

  public void setLocation(final GeodeticPosition location) {
    this.location = location;
  }

  public Double getVelocityX() {
    return velocityX;
  }

  public void setVelocityX(final Double velocityX) {
    this.velocityX = velocityX;
  }

  public Double getVelocityY() {
    return velocityY;
  }

  public void setVelocityY(final Double velocityY) {
    this.velocityY = velocityY;
  }

  public Double getVelocityZ() {
    return velocityZ;
  }

  public void setVelocityZ(final Double velocityZ) {
    this.velocityZ = velocityZ;
  }

  public Double getAccelerationX() {
    return accelerationX;
  }

  public void setAccelerationX(final Double accelerationX) {
    this.accelerationX = accelerationX;
  }

  public Double getAccelerationY() {
    return accelerationY;
  }

  public void setAccelerationY(final Double accelerationY) {
    this.accelerationY = accelerationY;
  }

  public Double getAccelerationZ() {
    return accelerationZ;
  }

  public void setAccelerationZ(final Double accelerationZ) {
    this.accelerationZ = accelerationZ;
  }

  public CovarianceMatrix getCovarianceMatrix() {
    return covarianceMatrix;
  }

  public void setCovarianceMatrix(final CovarianceMatrix covarianceMatrix) {
    this.covarianceMatrix = covarianceMatrix;
  }
}
