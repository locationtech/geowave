/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676.parser.model;

// STANAG 4676
/**
 * CovarianceMatrix Provides the matrix of covariances related to the estimated position vector and
 * the estimated position and velocity vectors.
 */
public class CovarianceMatrix {

  /**
   * Provides an estimate of the variance in the x component of position at the time of the report,
   * expressed in meters squared (m^2).
   */
  public Double covPosXPosX;

  /**
   * Provides an estimate of the variance in the y component of position at the time of the report,
   * expressed in meters squared (m^2).
   */
  public Double covPosYPosY;

  /**
   * Provides an estimate of the variance in the z component of position at the time of the report,
   * expressed in meters squared (m^2).
   */
  public Double covPosZPosZ;

  /**
   * Provides an estimate of the covariance between the x and y components of position, expressed in
   * meters squared (m^2).
   */
  public Double covPosXPosY;

  /**
   * Provides an estimate of the covariance between the x and z components of position, expressed in
   * meters squared (m^2).
   */
  public Double covPosXPosZ;

  /**
   * Provides an estimate of the covariance between the y and z components of position, expressed in
   * meters squared (m^2).
   */
  public Double covPosYPosZ;

  /**
   * Provides an estimate of the variance of the x component of velocity at the time of the report,
   * expressed in meters squared per seconds squared (m^2/s^2).
   */
  public Double covVelXVelX;

  /**
   * Provides an estimate of the variance of the y component of velocity at the time of the report,
   * expressed in meters squared per seconds squared (m^2/s^2).
   */
  public Double covVelYVelY;

  /**
   * Provides an estimate of the variance of the z component of velocity at the time of the report,
   * expressed in meters squared per seconds squared (m^2/s^2).
   */
  public Double covVelZVelZ;

  /**
   * Provides an estimate of the covariance between the x component of position and the x component
   * of velocity at the time of the report, expressed in meters squared per second (m^2/s).
   */
  public Double covPosXVelX;

  /**
   * Provides an estimate of the covariance between the x component of position and the y component
   * of velocity at the time of the report, expressed in meters squared per second (m^2/s).
   */
  public Double covPosXVelY;

  /**
   * Provides an estimate of the covariance between the x component of position and the z component
   * of velocity at the time of the report, expressed in meters squared per second (m^2/s).
   */
  public Double covPosXVelZ;

  /**
   * Provides an estimate of the covariance between the y component of position and the x component
   * of velocity at the time of the report, expressed in meters squared per second (m^2/s).
   */
  public Double covPosYVelX;

  /**
   * Provides an estimate of the covariance between the y component of position and the y component
   * of velocity at the time of the report, expressed in meters squared per second (m^2/s).
   */
  public Double covPosYVelY;

  /**
   * Provides an estimate of the covariance between the y component of position and the z component
   * of velocity at the time of the report, expressed in meters squared per second (m^2/s).
   */
  public Double covPosYVelZ;

  /**
   * Provides an estimate of the covariance between the z component of position and the x component
   * of velocity at the time of the report, expressed in meters squared per second (m^2/s).
   */
  public Double covPosZVelX;

  /**
   * Provides an estimate of the covariance between the z component of position and the y component
   * of velocity at the time of the report, expressed in meters squared per second (m^2/s).
   */
  public Double covPosZVelY;

  /**
   * Provides an estimate of the covariance between the z component of position and the z component
   * of velocity at the time of the report, expressed in meters squared per second (m^2/s).
   */
  public Double covPosZVelZ;

  /**
   * Provides an estimate of the covariance between the x and y components of velocity at the time
   * of the report, expressed in meters squared per seconds squared (m^2/s^2).
   */
  public Double covVelXVelY;

  /**
   * Provides an estimate of the covariance between the x and z components of velocity at the time
   * of the report, expressed in meters squared per seconds squared (m^2/s^2).
   */
  public Double covVelXVelZ;

  /**
   * Provides an estimate of the covariance between the y and z components of velocity at the time
   * of the report, expressed in meters squared per seconds squared (m^2/s^2).
   */
  public Double covVelYVelZ;

  public Double getCovPosXPosX() {
    return covPosXPosX;
  }

  public void setCovPosXPosX(final Double covPosXPosX) {
    this.covPosXPosX = covPosXPosX;
  }

  public Double getCovPosYPosY() {
    return covPosYPosY;
  }

  public void setCovPosYPosY(final Double covPosYPosY) {
    this.covPosYPosY = covPosYPosY;
  }

  public Double getCovPosZPosZ() {
    return covPosZPosZ;
  }

  public void setCovPosZPosZ(final Double covPosZPosZ) {
    this.covPosZPosZ = covPosZPosZ;
  }

  public Double getCovPosXPosY() {
    return covPosXPosY;
  }

  public void setCovPosXPosY(final Double covPosXPosY) {
    this.covPosXPosY = covPosXPosY;
  }

  public Double getCovPosXPosZ() {
    return covPosXPosZ;
  }

  public void setCovPosXPosZ(final Double covPosXPosZ) {
    this.covPosXPosZ = covPosXPosZ;
  }

  public Double getCovPosYPosZ() {
    return covPosYPosZ;
  }

  public void setCovPosYPosZ(final Double covPosYPosZ) {
    this.covPosYPosZ = covPosYPosZ;
  }

  public Double getCovVelXVelX() {
    return covVelXVelX;
  }

  public void setCovVelXVelX(final Double covVelXVelX) {
    this.covVelXVelX = covVelXVelX;
  }

  public Double getCovVelYVelY() {
    return covVelYVelY;
  }

  public void setCovVelYVelY(final Double covVelYVelY) {
    this.covVelYVelY = covVelYVelY;
  }

  public Double getCovVelZVelZ() {
    return covVelZVelZ;
  }

  public void setCovVelZVelZ(final Double covVelZVelZ) {
    this.covVelZVelZ = covVelZVelZ;
  }

  public Double getCovPosXVelX() {
    return covPosXVelX;
  }

  public void setCovPosXVelX(final Double covPosXVelX) {
    this.covPosXVelX = covPosXVelX;
  }

  public Double getCovPosXVelY() {
    return covPosXVelY;
  }

  public void setCovPosXVelY(final Double covPosXVelY) {
    this.covPosXVelY = covPosXVelY;
  }

  public Double getCovPosXVelZ() {
    return covPosXVelZ;
  }

  public void setCovPosXVelZ(final Double covPosXVelZ) {
    this.covPosXVelZ = covPosXVelZ;
  }

  public Double getCovPosYVelX() {
    return covPosYVelX;
  }

  public void setCovPosYVelX(final Double covPosYVelX) {
    this.covPosYVelX = covPosYVelX;
  }

  public Double getCovPosYVelY() {
    return covPosYVelY;
  }

  public void setCovPosYVelY(final Double covPosYVelY) {
    this.covPosYVelY = covPosYVelY;
  }

  public Double getCovPosYVelZ() {
    return covPosYVelZ;
  }

  public void setCovPosYVelZ(final Double covPosYVelZ) {
    this.covPosYVelZ = covPosYVelZ;
  }

  public Double getCovPosZVelX() {
    return covPosZVelX;
  }

  public void setCovPosZVelX(final Double covPosZVelX) {
    this.covPosZVelX = covPosZVelX;
  }

  public Double getCovPosZVelY() {
    return covPosZVelY;
  }

  public void setCovPosZVelY(final Double covPosZVelY) {
    this.covPosZVelY = covPosZVelY;
  }

  public Double getCovPosZVelZ() {
    return covPosZVelZ;
  }

  public void setCovPosZVelZ(final Double covPosZVelZ) {
    this.covPosZVelZ = covPosZVelZ;
  }

  public Double getCovVelXVelY() {
    return covVelXVelY;
  }

  public void setCovVelXVelY(final Double covVelXVelY) {
    this.covVelXVelY = covVelXVelY;
  }

  public Double getCovVelXVelZ() {
    return covVelXVelZ;
  }

  public void setCovVelXVelZ(final Double covVelXVelZ) {
    this.covVelXVelZ = covVelXVelZ;
  }

  public Double getCovVelYVelZ() {
    return covVelYVelZ;
  }

  public void setCovVelYVelZ(final Double covVelYVelZ) {
    this.covVelYVelZ = covVelYVelZ;
  }
}
