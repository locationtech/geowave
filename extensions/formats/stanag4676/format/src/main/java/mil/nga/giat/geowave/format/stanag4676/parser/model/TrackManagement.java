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

/**
 * information pertaining to track management functions.
 */
public class TrackManagement extends
		TrackItem
{
	private Long id;

	/**
	 * A track production area assigned to a tracker by a track data
	 * coordinator.
	 * <p>
	 * see AdatP-33 for TDL track management.
	 */
	public Area area;

	/**
	 * Information about the modality from which a track is computed.
	 * <p>
	 * See {@ModalityType}
	 */
	public ModalityType sourceModality;

	/**
	 * Information related to the environment in which a track is computed (i.e.
	 * land, air, space, etc). Compatible with Link 16 J3.5C1, environment field
	 * <p>
	 * See {@link TrackEnvironment}
	 */
	public TrackEnvironment environment;

	/**
	 * Quality of the track.
	 * <p>
	 * Allowed values from 0-15 in accordance of Link 16 J3.5, field Track
	 * Quality. Element can be used to support distributed track management.
	 */
	public int quality;

	/**
	 * Station ID of the tracker that produced the reported track
	 */
	public String stationId;

	/**
	 * Nationality of the tracker that produced the reported track
	 */
	public String nationality;

	/**
	 * Type of tracker that produced the reported track. See {@link TrackerType}
	 */
	public TrackerType trackerType;

	/**
	 * a flag to indicate an emergency situation, in accordance with Link 16
	 * Force Tell and Emergency indicator (J3.5).
	 */
	public boolean alertIndicator;

	public Long getId() {
		return id;
	}

	public void setId(
			Long id ) {
		this.id = id;
	}

	public Area getArea() {
		return area;
	}

	public void setArea(
			Area area ) {
		this.area = area;
	}

	public ModalityType getSourceModality() {
		return sourceModality;
	}

	public void setSourceModality(
			ModalityType sourceModality ) {
		this.sourceModality = sourceModality;
	}

	public TrackEnvironment getEnvironment() {
		return environment;
	}

	public void setEnvironment(
			TrackEnvironment environment ) {
		this.environment = environment;
	}

	public int getQuality() {
		return quality;
	}

	public void setQuality(
			int quality ) {
		this.quality = quality;
	}

	public String getStationId() {
		return stationId;
	}

	public void setStationId(
			String stationId ) {
		this.stationId = stationId;
	}

	public String getNationality() {
		return nationality;
	}

	public void setNationality(
			String nationality ) {
		this.nationality = nationality;
	}

	public TrackerType getTrackerType() {
		return trackerType;
	}

	public void setTrackerType(
			TrackerType trackerType ) {
		this.trackerType = trackerType;
	}

	public boolean getAlertIndicator() {
		return alertIndicator;
	}

	public void setAlertIndicator(
			boolean alertIndicator ) {
		this.alertIndicator = alertIndicator;
	}
}
