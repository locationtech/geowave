/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676.parser.model;

import java.util.UUID;

public class TrackDotSource {
  private Long id;
  private UUID gmtiUuid = null;
  private Double distance;

  public Long getId() {
    return id;
  }

  public void setId(final Long id) {
    this.id = id;
  }

  public UUID getGmtiUuid() {
    return gmtiUuid;
  }

  public void setGmtiUuid(final UUID gmtiUuid) {
    this.gmtiUuid = gmtiUuid;
  }

  public Double getDistance() {
    return distance;
  }

  public void setDistance(final Double distance) {
    this.distance = distance;
  }
}
