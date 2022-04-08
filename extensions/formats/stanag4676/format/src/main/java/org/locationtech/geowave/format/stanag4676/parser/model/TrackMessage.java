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
import java.util.UUID;

public class TrackMessage extends NATO4676Message {
  private Long id;

  private UUID uuid;
  private List<TrackEvent> tracks;
  private String missionId;

  public Long getId() {
    return id;
  }

  public void setId(final Long id) {
    this.id = id;
  }

  public UUID getUuid() {
    return uuid;
  }

  public void setUuid(final UUID uuid) {
    this.uuid = uuid;
  }

  public List<TrackEvent> getTracks() {
    return tracks;
  }

  public void setTracks(final List<TrackEvent> tracks) {
    this.tracks = tracks;
  }

  public void addTrackEvent(final TrackEvent trkEvnt) {
    if (tracks == null) {
      tracks = new ArrayList<>();
    }
    tracks.add(trkEvnt);
  }

  public void setMissionId(final String missionId) {
    this.missionId = missionId;
  }

  public String getMissionId() {
    return missionId;
  }
}
