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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Provides parameters related to a track.
 * <p>
 * Top-level information about the track is expressed in the Track class itself.
 */
public class TrackEvent
{
	private Long id;

	private String uuid;
	private String trackNumber;
	private TrackStatus status;
	private Security security;
	private String comment;
	private String missionId;
	private TreeMap<Long, TrackPoint> points = new TreeMap<Long, TrackPoint>();
	private TreeMap<Long, MotionEventPoint> motionEvents = new TreeMap<Long, MotionEventPoint>();
	private List<TrackIdentity> identities = new ArrayList<TrackIdentity>();
	private List<TrackClassification> classifications = new ArrayList<TrackClassification>();
	private List<TrackManagement> managements = new ArrayList<TrackManagement>();
	private List<MotionImagery> motionImages = new ArrayList<MotionImagery>();
	// private ESMInfo esm;
	private List<LineageRelation> trackRelations = new ArrayList<LineageRelation>();
	private ExerciseIndicator exerciseIndicator;
	private SimulationIndicator simulationIndicator;
	private Track track;

	public Long getId() {
		return id;
	}

	public void setId(
			final Long id ) {
		this.id = id;
	}

	public Track getTrack() {
		return track;
	}

	public void setTrack(
			final Track track ) {
		this.track = track;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(
			final String uuid ) {
		this.uuid = uuid;
	}

	public String getTrackNumber() {
		return trackNumber;
	}

	public void setTrackNumber(
			final String trackNumber ) {
		this.trackNumber = trackNumber;
	}

	public TrackStatus getStatus() {
		return status;
	}

	public void setStatus(
			final TrackStatus status ) {
		this.status = status;
	}

	public Security getSecurity() {
		return security;
	}

	public void setSecurity(
			final Security security ) {
		this.security = security;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(
			final String comment ) {
		this.comment = comment;
	}

	public String getMissionId() {
		return missionId;
	}

	public void setMissionId(
			final String missionId ) {
		this.missionId = missionId;
	}

	/**
	 * A list of the TrackPoints which comprise this track
	 * 
	 * @return A list of the TrackPoints which comprise this track
	 */
	public TreeMap<Long, TrackPoint> getPoints() {
		return points;
	}

	public TreeMap<Long, MotionEventPoint> getMotionPoints() {
		return motionEvents;
	}

	/**
	 * Sets the list of TrackPoints which comprise this track
	 * 
	 * @param points
	 *            the list of TrackPoints which comprise this track
	 */
	public void setPoints(
			final TreeMap<Long, TrackPoint> points ) {
		this.points = points;
	}

	/**
	 * Adds a TrackPoint to the list of TrackPoints comprise this track
	 * 
	 * @param point
	 *            the TrackPoint to add
	 */
	public void addPoint(
			final TrackPoint point ) {
		if (points == null) {
			points = new TreeMap<Long, TrackPoint>();
		}
		points.put(
				point.eventTime,
				point);
		if (track != null) {
			track.addPoint(point);
		}
	}

	/**
	 * Adds a TrackPoint to the list of TrackPoints comprise this track
	 * 
	 * @param point
	 *            the TrackPoint to add
	 */
	public void addMotionPoint(
			final MotionEventPoint point ) {
		if (motionEvents == null) {
			motionEvents = new TreeMap<Long, MotionEventPoint>();
		}
		motionEvents.put(
				point.eventTime,
				point);
		// if(motionEvents != null) {
		// track.addPoint(point);
		// }
	}

	/**
	 * Provides identity information about a track.
	 * <p>
	 * values are derived from STANAG 1241.
	 * 
	 * @return {@link TrackIdentity}
	 */
	public List<TrackIdentity> getIdentities() {
		return identities;
	}

	public void setIdentities(
			final List<TrackIdentity> identities ) {
		this.identities = identities;
		;
	}

	/**
	 * sets the identity information about this track
	 * 
	 * @param identity
	 *            {@link TrackIdentity}
	 */
	public void addIdentity(
			final TrackIdentity identity ) {
		if (identities == null) {
			identities = new ArrayList<TrackIdentity>();
		}
		identities.add(identity);
		if (track != null) {
			track.addIdentity(identity);
		}
	}

	/**
	 * Provides classification information about this track
	 * 
	 * @return {@link TrackClassification}
	 */
	public List<TrackClassification> getClassifications() {
		return classifications;
	}

	public void setClassifications(
			final List<TrackClassification> classifications ) {
		this.classifications = classifications;
		;
	}

	/**
	 * sets the classification information about this track
	 * 
	 * @param classification
	 *            {@link TrackClassificaion}
	 */
	public void addClassification(
			final TrackClassification classification ) {
		if (classifications == null) {
			classifications = new ArrayList<TrackClassification>();
		}
		classifications.add(classification);
		if (track != null) {
			track.addClassification(classification);
		}
	}

	/**
	 * Provides management information about this track
	 * 
	 * @return {@link TrackManagement}
	 */
	public List<TrackManagement> getManagements() {
		return managements;
	}

	public void setManagements(
			final List<TrackManagement> managements ) {
		this.managements = managements;
		;
	}

	/**
	 * sets the management information about this track
	 * 
	 * @param management
	 *            {@link TrackManagement}
	 */
	public void addManagement(
			final TrackManagement management ) {
		if (managements == null) {
			managements = new ArrayList<TrackManagement>();
		}
		managements.add(management);
		if (track != null) {
			track.addManagement(management);
		}
	}

	/**
	 * Provides video (motion imagery) information about this track
	 * 
	 * @return {@link MotionImagery}
	 */
	public List<MotionImagery> getMotionImages() {
		return motionImages;
	}

	public void setMotionImages(
			final List<MotionImagery> motionImages ) {
		this.motionImages = motionImages;
		;
	}

	/**
	 * sets the management information about this track
	 * 
	 * @param management
	 *            {@link MotionImagery}
	 */
	public void addMotionImagery(
			final MotionImagery image ) {
		if (motionImages == null) {
			motionImages = new ArrayList<MotionImagery>();
		}
		motionImages.add(image);
		if (track != null) {
			track.addMotionImagery(image);
		}
	}

	/**
	 * Provides a list of related tracks
	 * 
	 * @return List<{@link LineageRelation}>
	 */
	public List<LineageRelation> getTrackRelations() {
		return trackRelations;
	}

	public void setTrackRelations(
			final List<LineageRelation> trackRelations ) {
		this.trackRelations = trackRelations;
		;
	}

	/**
	 * sets a list of related tracks
	 * 
	 * @param trackRelations
	 *            List<{@link LineageRelation}>
	 */
	public void addTrackRelation(
			final LineageRelation relation ) {
		if (trackRelations == null) {
			trackRelations = new ArrayList<LineageRelation>();
		}
		trackRelations.add(relation);
		if (track != null) {
			track.addTrackRelation(relation);
		}
	}

	public void setExerciseIndicator(
			final ExerciseIndicator exerciseIndicator ) {
		this.exerciseIndicator = exerciseIndicator;
	}

	public ExerciseIndicator getExerciseIndicator() {
		return exerciseIndicator;
	}

	public void setSimulationIndicator(
			final SimulationIndicator simulationIndicator ) {
		this.simulationIndicator = simulationIndicator;
	}

	public SimulationIndicator getSimulationIndicator() {
		return simulationIndicator;
	}

}
