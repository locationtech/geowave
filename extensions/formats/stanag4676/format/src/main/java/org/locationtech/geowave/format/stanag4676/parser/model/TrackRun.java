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

public class TrackRun {
  private Long id;
  private UUID uuid;
  private String algorithm;
  private String algorithmVersion;
  private long runDate;
  private String userId;
  private String comment;
  private String sourceFilename;
  private Long sourceGmtiMissionUid;
  private UUID sourceGmtiMissionUuid;
  private List<TrackRunParameter> parameters = new ArrayList<>();
  private List<NATO4676Message> messages = new ArrayList<>();

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

  public String getAlgorithm() {
    return algorithm;
  }

  public void setAlgorithm(final String algorithm) {
    this.algorithm = algorithm;
  }

  public String getAlgorithmVersion() {
    return algorithmVersion;
  }

  public void setAlgorithmVersion(final String algorithmVersion) {
    this.algorithmVersion = algorithmVersion;
  }

  public long getRunDate() {
    return runDate;
  }

  public void setRunDate(final long runDate) {
    this.runDate = runDate;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(final String userId) {
    this.userId = userId;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(final String comment) {
    this.comment = comment;
  }

  public Long getSourceGmtiMissionUid() {
    return sourceGmtiMissionUid;
  }

  public void setSourceGmtiMissionUid(final Long sourceGmtiMissionUid) {
    this.sourceGmtiMissionUid = sourceGmtiMissionUid;
  }

  public UUID getSourceGmtiMissionUuid() {
    return sourceGmtiMissionUuid;
  }

  public void setSourceGmtiMissionUuid(final UUID sourceGmtiMissionUuid) {
    this.sourceGmtiMissionUuid = sourceGmtiMissionUuid;
  }

  public List<TrackRunParameter> getParameters() {
    return parameters;
  }

  public void setParameters(final List<TrackRunParameter> parameters) {
    this.parameters = parameters;
  }

  public List<NATO4676Message> getMessages() {
    return messages;
  }

  public void setMessages(final List<NATO4676Message> messages) {
    this.messages = messages;
  }

  public void addParameter(final TrackRunParameter param) {
    if (parameters == null) {
      parameters = new ArrayList<>();
    }
    parameters.add(param);
  }

  public void addParameter(final String name, final String value) {
    addParameter(new TrackRunParameter(name, value));
  }

  public void clearParameters() {
    if (parameters == null) {
      parameters = new ArrayList<>();
    }
    parameters.clear();
  }

  public void addMessage(final NATO4676Message message) {
    if (messages == null) {
      messages = new ArrayList<>();
    }
    messages.add(message);
  }

  public void clearMessages() {
    if (messages == null) {
      messages = new ArrayList<>();
    }
    messages.clear();
  }

  public void setSourceFilename(final String sourceFilename) {
    this.sourceFilename = sourceFilename;
  }

  public String getSourceFilename() {
    return sourceFilename;
  }
}
