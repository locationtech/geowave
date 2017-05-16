/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.stanag4676.parser.model;

import java.util.List;
import java.util.ArrayList;
import java.util.UUID;

public class TrackRun
{
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
	private List<TrackRunParameter> parameters = new ArrayList<TrackRunParameter>();
	private List<NATO4676Message> messages = new ArrayList<NATO4676Message>();

	public Long getId() {
		return id;
	}

	public void setId(
			Long id ) {
		this.id = id;
	}

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(
			UUID uuid ) {
		this.uuid = uuid;
	}

	public String getAlgorithm() {
		return algorithm;
	}

	public void setAlgorithm(
			String algorithm ) {
		this.algorithm = algorithm;
	}

	public String getAlgorithmVersion() {
		return algorithmVersion;
	}

	public void setAlgorithmVersion(
			String algorithmVersion ) {
		this.algorithmVersion = algorithmVersion;
	}

	public long getRunDate() {
		return runDate;
	}

	public void setRunDate(
			long runDate ) {
		this.runDate = runDate;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(
			String userId ) {
		this.userId = userId;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(
			String comment ) {
		this.comment = comment;
	}

	public Long getSourceGmtiMissionUid() {
		return sourceGmtiMissionUid;
	}

	public void setSourceGmtiMissionUid(
			Long sourceGmtiMissionUid ) {
		this.sourceGmtiMissionUid = sourceGmtiMissionUid;
	}

	public UUID getSourceGmtiMissionUuid() {
		return sourceGmtiMissionUuid;
	}

	public void setSourceGmtiMissionUuid(
			UUID sourceGmtiMissionUuid ) {
		this.sourceGmtiMissionUuid = sourceGmtiMissionUuid;
	}

	public List<TrackRunParameter> getParameters() {
		return parameters;
	}

	public void setParameters(
			List<TrackRunParameter> parameters ) {
		this.parameters = parameters;
	}

	public List<NATO4676Message> getMessages() {
		return messages;
	}

	public void setMessages(
			List<NATO4676Message> messages ) {
		this.messages = messages;
	}

	public void addParameter(
			TrackRunParameter param ) {
		if (this.parameters == null) {
			this.parameters = new ArrayList<TrackRunParameter>();
		}
		this.parameters.add(param);
	}

	public void addParameter(
			String name,
			String value ) {
		addParameter(new TrackRunParameter(
				name,
				value));
	}

	public void clearParameters() {
		if (this.parameters == null) {
			this.parameters = new ArrayList<TrackRunParameter>();
		}
		this.parameters.clear();
	}

	public void addMessage(
			NATO4676Message message ) {
		if (this.messages == null) {
			this.messages = new ArrayList<NATO4676Message>();
		}
		this.messages.add(message);
	}

	public void clearMessages() {
		if (this.messages == null) {
			this.messages = new ArrayList<NATO4676Message>();
		}
		this.messages.clear();
	}

	public void setSourceFilename(
			String sourceFilename ) {
		this.sourceFilename = sourceFilename;
	}

	public String getSourceFilename() {
		return sourceFilename;
	}
}
