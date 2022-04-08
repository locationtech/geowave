/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.spatial;

import org.locationtech.geowave.analytic.spark.GeoWaveRDD;

public abstract class JoinStrategy implements SpatialJoin {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  // Final joined pair RDDs
  protected GeoWaveRDD leftJoined = null;
  protected GeoWaveRDD rightJoined = null;

  protected JoinOptions joinOpts = new JoinOptions();

  public GeoWaveRDD getLeftResults() {
    return leftJoined;
  }

  public void setLeftResults(final GeoWaveRDD leftJoined) {
    this.leftJoined = leftJoined;
  }

  public GeoWaveRDD getRightResults() {
    return rightJoined;
  }

  public void setRightResults(final GeoWaveRDD rightJoined) {
    this.rightJoined = rightJoined;
  }

  public JoinOptions getJoinOptions() {
    return joinOpts;
  }

  public void setJoinOptions(final JoinOptions joinOpts) {
    this.joinOpts = joinOpts;
  }
}
