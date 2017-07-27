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

public class Track
{
	private Long id;

	private String uuid;
	private String trackNumber;

	private TrackStatus status;
	private Security security;
	private String comment;
	private List<TrackPoint> points = new ArrayList<TrackPoint>();
	private List<TrackIdentity> identities = new ArrayList<TrackIdentity>();
	private List<TrackClassification> classifications = new ArrayList<TrackClassification>();
	private List<TrackManagement> managements = new ArrayList<TrackManagement>();
	private List<MotionImagery> motionImages;
	private List<LineageRelation> trackRelations = new ArrayList<LineageRelation>();

	public Long getId() {
		return id;
	}

	public void setId(
			Long id ) {
		this.id = id;
	}

	/**
	 * The UUID of a track
	 * 
	 * @return UUID
	 */
	public String getUuid() {
		return uuid;
	}

	/**
	 * Sets the UUID of the track
	 * 
	 * @param uuid
	 */
	public void setUuid(
			String uuid ) {
		this.uuid = uuid;
	}

	public String getTrackNumber() {
		return trackNumber;
	}

	public void setTrackNumber(
			String trackNumber ) {
		this.trackNumber = trackNumber;
	}

	public TrackStatus getStatus() {
		return status;
	}

	public void setStatus(
			TrackStatus status ) {
		this.status = status;
	}

	public Security getSecurity() {
		return security;
	}

	public void setSecurity(
			Security security ) {
		this.security = security;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(
			String comment ) {
		this.comment = comment;
	}

	/**
	 * A list of the TrackPoints which comprise this track
	 * 
	 * @return A list of the TrackPoints which comprise this track
	 */
	public List<TrackPoint> getPoints() {
		return points;
	}

	/**
	 * Sets the list of TrackPoints which comprise this track
	 * 
	 * @param points
	 *            the list of TrackPoints which comprise this track
	 */
	public void setPoints(
			List<TrackPoint> points ) {
		this.points = points;
	}

	/**
	 * Adds a TrackPoint to the list of TrackPoints comprise this track
	 * 
	 * @param point
	 *            the TrackPoint to add
	 */
	public void addPoint(
			TrackPoint point ) {
		if (this.points == null) {
			this.points = new ArrayList<TrackPoint>();
		}
		this.points.add(point);
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
			List<TrackIdentity> identities ) {
		this.identities = identities;
	}

	/**
	 * sets the identity information about this track
	 * 
	 * @param identity
	 *            {@link TrackIdentity}
	 */
	public void addIdentity(
			TrackIdentity identity ) {
		if (this.identities == null) {
			this.identities = new ArrayList<TrackIdentity>();
		}
		this.identities.add(identity);
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
			List<TrackClassification> classifications ) {
		this.classifications = classifications;
	}

	/**
	 * sets the classification information about this track
	 * 
	 * @param classification
	 *            {@link TrackClassificaion}
	 */
	public void addClassification(
			TrackClassification classification ) {
		if (this.classifications == null) {
			this.classifications = new ArrayList<TrackClassification>();
		}
		this.classifications.add(classification);
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
			List<TrackManagement> managements ) {
		this.managements = managements;
	}

	/**
	 * sets the management information about this track
	 * 
	 * @param management
	 *            {@link TrackManagement}
	 */
	public void addManagement(
			TrackManagement management ) {
		if (this.managements == null) {
			this.managements = new ArrayList<TrackManagement>();
		}
		this.managements.add(management);
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
			List<LineageRelation> trackRelations ) {
		this.trackRelations = trackRelations;
	}

	/**
	 * sets a list of related tracks
	 * 
	 * @param trackRelations
	 *            List<{@link LineageRelation}>
	 */
	public void addTrackRelation(
			LineageRelation relation ) {
		if (this.trackRelations == null) {
			this.trackRelations = new ArrayList<LineageRelation>();
		}
		this.trackRelations.add(relation);
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
			List<MotionImagery> motionImages ) {
		this.motionImages = motionImages;
	}

	public void addMotionImagery(
			MotionImagery image ) {
		if (this.motionImages == null) {
			this.motionImages = new ArrayList<MotionImagery>();
		}
		this.motionImages.add(image);
	}

}
