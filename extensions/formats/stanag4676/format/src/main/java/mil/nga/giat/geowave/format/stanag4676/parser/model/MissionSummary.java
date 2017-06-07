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

public class MissionSummary
{
	private Area coverageArea;
	private String missionId;
	private String name;
	private String security;
	private long startTime;
	private long endTime;

	private List<MissionFrame> frames = new ArrayList<MissionFrame>();
	private List<ObjectClassification> classifications = new ArrayList<ObjectClassification>();

	/**
	 * @return the missionId
	 */
	public String getMissionId() {
		return missionId;
	}

	/**
	 * @param missionId
	 *            the missionId to set
	 */
	public void setMissionId(
			String missionId ) {
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
	public void setName(
			String name ) {
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
	public void setSecurity(
			String security ) {
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
	 * @param frames
	 *            the list of frames which comprise this mission
	 */
	public void setFrames(
			List<MissionFrame> frames ) {
		this.frames = frames;
	}

	/**
	 * Adds a MissionFrame
	 * 
	 * @param frame
	 *            the MissionFrame to add
	 */
	public void addFrame(
			MissionFrame frame ) {
		if (this.frames == null) {
			this.frames = new ArrayList<MissionFrame>();
		}
		this.frames.add(frame);
	}

	/**
	 * Provides object classification information about this mission
	 * 
	 * @return {@link ObjectClassification}
	 */
	public List<ObjectClassification> getClassifications() {
		return classifications;
	}

	public void setClassifications(
			List<ObjectClassification> classifications ) {
		this.classifications = classifications;
	}

	/**
	 * sets the object classification information about this mission
	 * 
	 * @param classification
	 *            {@link ObjectClassification}
	 */
	public void addClassification(
			ObjectClassification classification ) {
		if (this.classifications == null) {
			this.classifications = new ArrayList<ObjectClassification>();
		}
		this.classifications.add(classification);
	}

	/**
	 * @return the startTime
	 */
	public long getStartTime() {
		return startTime;
	}

	/**
	 * @param startTime
	 *            the startTime to set
	 */
	public void setStartTime(
			long startTime ) {
		this.startTime = startTime;
	}

	/**
	 * @return the endTime
	 */
	public long getEndTime() {
		return endTime;
	}

	/**
	 * @param endTime
	 *            the endTime to set
	 */
	public void setEndTime(
			long endTime ) {
		this.endTime = endTime;
	}

	/**
	 * @return the coverageArea
	 */
	public Area getCoverageArea() {
		return coverageArea;
	}

	/**
	 * @param coverageArea
	 *            the coverageArea to set
	 */
	public void setCoverageArea(
			Area coverageArea ) {
		this.coverageArea = coverageArea;
	}

}
