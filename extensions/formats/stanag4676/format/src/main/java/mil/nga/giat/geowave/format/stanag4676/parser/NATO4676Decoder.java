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
package mil.nga.giat.geowave.format.stanag4676.parser;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import mil.nga.giat.geowave.format.stanag4676.parser.model.Area;
import mil.nga.giat.geowave.format.stanag4676.parser.model.ClassificationCredibility;
import mil.nga.giat.geowave.format.stanag4676.parser.model.ClassificationLevel;
import mil.nga.giat.geowave.format.stanag4676.parser.model.CovarianceMatrix;
import mil.nga.giat.geowave.format.stanag4676.parser.model.ExerciseIndicator;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MissionFrame;
import mil.nga.giat.geowave.format.stanag4676.parser.model.GeodeticPosition;
import mil.nga.giat.geowave.format.stanag4676.parser.model.IDdata;
import mil.nga.giat.geowave.format.stanag4676.parser.model.Identity;
import mil.nga.giat.geowave.format.stanag4676.parser.model.LineageRelation;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MissionSummary;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MissionSummaryMessage;
import mil.nga.giat.geowave.format.stanag4676.parser.model.ModalityType;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MotionEventPoint;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MotionImagery;
import mil.nga.giat.geowave.format.stanag4676.parser.model.NATO4676Message;
import mil.nga.giat.geowave.format.stanag4676.parser.model.ObjectClassification;
import mil.nga.giat.geowave.format.stanag4676.parser.model.Security;
import mil.nga.giat.geowave.format.stanag4676.parser.model.SimulationIndicator;
import mil.nga.giat.geowave.format.stanag4676.parser.model.SymbolicSpectralRange;
import mil.nga.giat.geowave.format.stanag4676.parser.model.Track;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackClassification;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackEnvironment;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackEvent;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackIdentity;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackManagement;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackMessage;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackPoint;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackPointDetail;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackPointType;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackStatus;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackerType;

import org.apache.commons.io.IOUtils;
import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.JDOMParseException;
import org.jdom.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NATO4676Decoder implements
		TrackDecoder
{

	HashMap<String, Track> trackMap = new HashMap<String, Track>();
	private int trackStatsNumTracks = 0;
	private int trackStatsNumDots = 0;
	private static final Logger LOGGER = LoggerFactory.getLogger(NATO4676Decoder.class);

	boolean printNotParse = false;

	@Override
	public void initialize() {
		trackStatsNumTracks = 0;
		trackStatsNumDots = 0;
	}

	public void setPrintNotParse(
			final boolean shouldPrint ) {
		printNotParse = shouldPrint;
	}

	@Override
	public NATO4676Message readNext(
			final InputStream is ) {
		NATO4676Message msg = null;
		try {
			if (printNotParse) {
				final String trackStr = IOUtils.toString(
						is,
						"UTF-8");
				is.reset();
			}
			else {
				final SAXBuilder builder = new SAXBuilder();
				final Document doc = builder.build(is);

				final Element rootEl = doc.getRootElement();
				final Namespace xmlns = rootEl.getNamespace();

				String name = rootEl.getName();
				if ("TrackMessage".equals(name)) {
					msg = readTrackMessage(
							rootEl,
							xmlns);
					LOGGER.info("TrackMessage read " + trackStatsNumTracks + " Tracks and " + trackStatsNumDots
							+ " TrackPoints.");
				}
				else if ("MissionSummary".equals(name)) {
					msg = readMissionSummaryMessage(
							rootEl,
							xmlns);
				}
			}
		}
		catch (final JDOMParseException jdomPe) {
			LOGGER.info(
					"jdomParseException: " + jdomPe.getLocalizedMessage(),
					jdomPe);
			return null;
		}
		catch (final IOException ioe) {
			LOGGER.info(
					"IO exception: " + ioe.getLocalizedMessage(),
					ioe);
			return null;
		}
		catch (final JDOMException jdome) {
			LOGGER.info(
					"jdomException: " + jdome.getLocalizedMessage(),
					jdome);
			return null;
		}

		return msg;
	}

	private MissionSummaryMessage readMissionSummaryMessage(
			final Element element,
			final Namespace xmlns ) {
		final MissionSummaryMessage msg = new MissionSummaryMessage();
		MissionSummary missionSummary = msg.getMissionSummary();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("missionID".equals(childName)) {
				missionSummary.setMissionId(childValue);
			}
			else if ("Name".equals(childName)) {
				missionSummary.setName(childValue);
			}
			else if ("Security".equals(childName)) {
				msg.setSecurity(readSecurity(
						child,
						xmlns));
				missionSummary.setSecurity(msg.getSecurity().getClassification().toString());
			}
			else if ("msgCreatedTime".equals(childName)) {
				msg.setMessageTime(DateStringToLong(childValue));
			}
			else if ("senderId".equals(childName)) {
				msg.setSenderID(readIDdata(
						child,
						xmlns));
			}
			else if ("StartTime".equals(childName)) {
				missionSummary.setStartTime(DateStringToLong(childValue));
			}
			else if ("EndTime".equals(childName)) {
				missionSummary.setEndTime(DateStringToLong(childValue));
			}
			else if ("FrameInformation".equals(childName)) {
				missionSummary.addFrame(readFrame(
						child,
						xmlns));
			}
			else if ("CoverageArea".equals(childName)) {
				missionSummary.setCoverageArea(readCoverageArea(
						child,
						xmlns));
			}
			else if ("ActiveObjectClassifications".equals(childName)) {
				missionSummary.setClassifications(readObjectClassifications(
						child,
						xmlns));
			}
		}
		return msg;
	}

	private List<ObjectClassification> readObjectClassifications(
			final Element element,
			final Namespace xmlns ) {
		final List<ObjectClassification> objClassList = new ArrayList<ObjectClassification>();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();

			if ("classification".equals(childName)) {
				ObjectClassification classification = ObjectClassification.fromString(childValue);
				if (classification != null) objClassList.add(classification);
			}
		}

		return objClassList;
	}

	private MissionFrame readFrame(
			final Element element,
			final Namespace xmlns ) {
		final MissionFrame frame = new MissionFrame();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("frameNumber".equals(childName)) {
				frame.setFrameNumber(Integer.parseInt(childValue));
			}
			else if ("frameTimestamp".equals(childName)) {
				frame.setFrameTime(DateStringToLong(childValue));
			}
			else if ("frameCoverageArea".equals(childName)) {
				frame.setCoverageArea(readCoverageArea(
						child,
						xmlns));
			}
		}
		return frame;
	}

	private TrackMessage readTrackMessage(
			final Element element,
			final Namespace xmlns ) {
		final TrackMessage msg = new TrackMessage();
		msg.setUuid(UUID.randomUUID());
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("stanagVersion".equals(childName)) {
				msg.setFormatVersion(childValue);
			}
			else if ("messageSecurity".equals(childName)) {
				msg.setSecurity(readSecurity(
						child,
						xmlns));
			}
			else if ("msgCreatedTime".equals(childName)) {
				msg.setMessageTime(DateStringToLong(childValue));
			}
			else if ("senderId".equals(childName)) {
				msg.setSenderID(readIDdata(
						child,
						xmlns));
			}
			else if ("tracks".equals(childName)) {
				msg.addTrackEvent(readTrackEvent(
						child,
						xmlns));
			}

		}
		return msg;
	}

	private Security readSecurity(
			final Element element,
			final Namespace xmlns ) {
		final Security security = new Security();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("securityClassification".equals(childName)) {
				try {
					security.setClassification(ClassificationLevel.valueOf(childValue));
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set classification level",
							iae);
					security.setClassification(null);
				}
			}
			if ("securityPolicyName".equals(childName)) {
				security.setPolicyName(childValue);
			}
			if ("securityControlSystem".equals(childName)) {
				security.setControlSystem(childValue);
			}
			if ("securityDissemination".equals(childName)) {
				security.setDissemination(childValue);
			}
			if ("securityReleasability".equals(childName)) {
				security.setReleasability(childValue);
			}
		}
		return security;
	}

	private IDdata readIDdata(
			final Element element,
			final Namespace xmlns ) {
		final IDdata id = new IDdata();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("stationID".equals(childName)) {
				id.setStationId(childValue);
			}
			else if ("nationality".equals(childName)) {
				id.setNationality(childValue);
			}
		}
		return id;
	}

	private TrackEvent readTrackEvent(
			final Element element,
			final Namespace xmlns ) {
		final TrackEvent trackEvent = new TrackEvent();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("trackUUID".equals(childName)) {
				trackEvent.setUuid(childValue);
			}
			else if ("trackNumber".equals(childName)) {
				trackEvent.setTrackNumber(childValue);
			}
			else if ("trackStatus".equals(childName)) {
				try {
					trackEvent.setStatus(TrackStatus.valueOf(childValue));
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set status",
							iae);
					trackEvent.setStatus(null);
				}
			}
			else if ("trackSecurity".equals(childName)) {
				trackEvent.setSecurity(readSecurity(
						child,
						xmlns));
			}
			else if ("trackComment".equals(childName)) {
				trackEvent.setComment(childValue);
			}
			else if ("missionID".equals(childName)) {
				trackEvent.setMissionId(childValue);
			}
			else if ("exerciseIndicator".equals(childName)) {
				try {
					trackEvent.setExerciseIndicator(ExerciseIndicator.valueOf(childValue));
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set exercise indicator",
							iae);
					trackEvent.setExerciseIndicator(null);
				}
			}
			else if ("simulationIndicator".equals(childName)) {
				try {
					trackEvent.setSimulationIndicator(SimulationIndicator.valueOf(childValue));
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set simulation indicator",
							iae);
					trackEvent.setSimulationIndicator(null);
				}
			}
			else if ("items".equals(childName)) {
				final Namespace xsi = Namespace.getNamespace(
						"xsi",
						"http://www.w3.org/2001/XMLSchema-instance");
				final Attribute xsitype = child.getAttribute(
						"type",
						xsi);
				if (xsitype != null) {
					if ("TrackPoint".equals(xsitype.getValue())) {
						trackEvent.addPoint(readTrackPoint(
								child,
								xmlns));
					}
					else if ("TrackIdentityInformation".equals(xsitype.getValue())) {
						trackEvent.addIdentity(readTrackIdentity(
								child,
								xmlns));
					}
					else if ("TrackClassificationInformation".equals(xsitype.getValue())) {
						trackEvent.addClassification(readTrackClassification(
								child,
								xmlns));
					}
					else if ("TrackManagementInformation".equals(xsitype.getValue())) {
						trackEvent.addManagement(readTrackManagement(
								child,
								xmlns));
					}
					else if ("VideoInformation".equals(xsitype.getValue())) {
						trackEvent.addMotionImagery(readMotionImagery(
								child,
								xmlns));
					}
					else if ("ESMInformation".equals(xsitype.getValue())) {
						// TODO: ESM not implemented yet.
					}
					else if ("TrackLineageInformation".equals(xsitype.getValue())) {
						trackEvent.addTrackRelation(readLineageRelation(
								child,
								xmlns));
					}
					else if ("MotionEventInformation".equals(xsitype.getValue())) {
						trackEvent.addMotionPoint(readMotionPoint(
								child,
								xmlns));
					}
				}
				else {
					final TrackPoint point = readTrackPoint(
							child,
							xmlns);
					if (point != null) {
						trackEvent.addPoint(point);
					}
				}
			}
		}
		Track track = trackMap.get(trackEvent.getTrackNumber());
		if (track == null) {
			track = new Track();
			track.setUuid(trackEvent.getUuid()); // don't need to fully populate
													// the Track object
			trackMap.put(
					trackEvent.getTrackNumber(),
					track);
			trackStatsNumTracks++;
		}
		return trackEvent;
	}

	private TrackPoint readTrackPoint(
			final Element element,
			final Namespace xmlns ) {
		final TrackPoint trackPoint = new TrackPoint();
		trackStatsNumDots++;
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("trackItemUUID".equals(childName)) {
				trackPoint.uuid = childValue;
			}
			else if ("trackItemSecurity".equals(childName)) {
				trackPoint.security = readSecurity(
						child,
						xmlns);
			}
			else if ("trackItemTime".equals(childName)) {
				trackPoint.eventTime = DateStringToLong(childValue);
			}
			else if ("trackItemSource".equals(childName)) {
				trackPoint.trackItemSource = childValue;
			}
			else if ("trackItemComment".equals(childName)) {
				trackPoint.trackItemComment = childValue;
			}
			else if ("trackPointPosition".equals(childName)) {
				trackPoint.location = readGeodeticPosition(
						child,
						xmlns);
			}
			else if ("trackPointSpeed".equals(childName)) {
				try {
					trackPoint.speed = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					LOGGER.warn(
							"Unable to set speed",
							nfe);
					trackPoint.speed = null;
				}
			}
			else if ("motionEvent".equals(childName) && (childValue != null)) {
				trackPoint.motionEvent = childValue.trim();
			}
			else if ("motionEventPosition".equals(childName)) {
				trackPoint.location = readGeodeticPosition(
						child,
						xmlns);
			}
			else if ("trackPointCourse".equals(childName)) {
				try {
					trackPoint.course = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					LOGGER.warn(
							"Unable to set course",
							nfe);
					trackPoint.course = null;
				}
			}
			else if ("trackPointType".equals(childName)) {
				try {
					trackPoint.trackPointType = TrackPointType.valueOf(childValue);
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set track point type",
							iae);
					trackPoint.trackPointType = null;
				}
			}
			else if ("trackPointSource".equals(childName)) {
				trackPoint.trackPointSource = ModalityType.fromString(childValue);
			}
			else if ("trackPointObjectMask".equals(childName)) {
				trackPoint.objectMask = readArea(
						child,
						xmlns);
			}
			else if ("TrackPointDetail".equals(childName)) {
				trackPoint.detail = readTrackPointDetail(
						child,
						xmlns);
			}
		}
		return trackPoint;
	}

	private MotionEventPoint readMotionPoint(
			final Element element,
			final Namespace xmlns ) {
		final MotionEventPoint trackPoint = new MotionEventPoint();
		// trackStatsNumDots++;
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("trackItemUUID".equals(childName)) {
				trackPoint.uuid = childValue;
			}
			else if ("trackItemSecurity".equals(childName)) {
				trackPoint.security = readSecurity(
						child,
						xmlns);
			}
			else if ("trackItemTime".equals(childName)) {
				trackPoint.eventTime = DateStringToLong(childValue);
			}
			else if ("trackItemSource".equals(childName)) {
				trackPoint.trackItemSource = childValue;
			}
			else if ("trackItemComment".equals(childName)) {
				trackPoint.trackItemComment = childValue;
			}
			else if ("trackPointPosition".equals(childName)) {
				trackPoint.location = readGeodeticPosition(
						child,
						xmlns);
			}
			else if ("trackPointSpeed".equals(childName)) {
				try {
					trackPoint.speed = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					LOGGER.warn(
							"Unable to set speed",
							nfe);
					trackPoint.speed = null;
				}
			}
			else if ("motionEvent".equals(childName) && (childValue != null)) {
				trackPoint.motionEvent = childValue.trim();
			}
			else if ("motionEventEndTime".equals(childName) && (childValue != null)) {
				trackPoint.eventEndTime = DateStringToLong(childValue);
			}
			else if ("motionEventPosition".equals(childName)) {
				trackPoint.location = readGeodeticPosition(
						child,
						xmlns);
			}
			else if ("trackPointCourse".equals(childName)) {
				try {
					trackPoint.course = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					LOGGER.warn(
							"Unable to set course",
							nfe);
					trackPoint.course = null;
				}
			}
			else if ("trackPointType".equals(childName)) {
				try {
					trackPoint.trackPointType = TrackPointType.valueOf(childValue);
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set track point type",
							iae);
					trackPoint.trackPointType = null;
				}
			}
			else if ("trackPointSource".equals(childName)) {
				try {
					trackPoint.trackPointSource = ModalityType.valueOf(childValue);
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set track point source",
							iae);
					trackPoint.trackPointSource = null;
				}
			}
			else if ("trackPointObjectMask".equals(childName)) {
				trackPoint.objectMask = readArea(
						child,
						xmlns);
			}
			else if ("TrackPointDetail".equals(childName)) {
				trackPoint.detail = readTrackPointDetail(
						child,
						xmlns);
			}
		}
		return trackPoint;
	}

	private TrackIdentity readTrackIdentity(
			final Element element,
			final Namespace xmlns ) {
		final TrackIdentity trackIdentity = new TrackIdentity();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("identity".equals(childName)) {
				try {
					trackIdentity.identity = Identity.valueOf(childValue);
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set identity",
							iae);
					trackIdentity.identity = null;
				}

			}
			// TODO: Track Identity
		}
		return trackIdentity;
	}

	private TrackClassification readTrackClassification(
			final Element element,
			final Namespace xmlns ) {
		final TrackClassification trackClassification = new TrackClassification();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("trackItemUUID".equals(childName)) {
				try {
					trackClassification.setUuid(UUID.fromString(childValue));
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set uuid",
							iae);
					trackClassification.setUuid(null);
				}
			}
			else if ("trackItemSecurity".equals(childName)) {
				trackClassification.setSecurity(readSecurity(
						child,
						xmlns));
			}
			else if ("trackItemTime".equals(childName)) {
				trackClassification.setTime(DateStringToLong(childValue));
			}
			else if ("trackItemSource".equals(childName)) {
				trackClassification.setSource(childValue);
			}
			else if ("classification".equals(childName)) {
				trackClassification.classification = ObjectClassification.fromString(childValue);
			}
			else if ("classificationCredibility".equals(childName)) {
				trackClassification.credibility = readClassificationCredibility(
						child,
						xmlns);
			}
			else if ("numObjects".equals(childName)) {
				trackClassification.setNumObjects(Integer.parseInt(child.getText()));
			}
		}
		return trackClassification;
	}

	private TrackManagement readTrackManagement(
			final Element element,
			final Namespace xmlns ) {
		final TrackManagement trackManagement = new TrackManagement();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("trackItemUUID".equals(childName)) {
				try {
					trackManagement.setUuid(UUID.fromString(childValue));
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set uuid",
							iae);
					trackManagement.setUuid(null);
				}
			}
			else if ("trackItemSecurity".equals(childName)) {
				trackManagement.setSecurity(readSecurity(
						child,
						xmlns));
			}
			else if ("trackItemTime".equals(childName)) {
				trackManagement.setTime(DateStringToLong(childValue));
			}
			else if ("trackItemSource".equals(childName)) {
				trackManagement.setSource(childValue);
			}
			else if ("trackItemComment".equals(childName)) {
				trackManagement.setComment(childValue);
			}
			else if ("trackProductionArea".equals(childName)) {
				trackManagement.area = readArea(
						child,
						xmlns);
			}
			else if ("trackSource".equals(childName)) {
				try {
					trackManagement.sourceModality = ModalityType.valueOf(childValue);
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set source modality",
							iae);
					trackManagement.sourceModality = null;
				}
			}
			else if ("trackEnvironment".equals(childName)) {
				try {
					trackManagement.environment = TrackEnvironment.valueOf(childValue);
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set environment",
							iae);
					trackManagement.environment = null;
				}
			}
			else if ("trackQuality".equals(childName)) {
				try {
					trackManagement.quality = Integer.parseInt(childValue);
				}
				catch (final NumberFormatException nfe) {
					trackManagement.quality = 0;
				}
			}
			else if ("trackerID".equals(childName)) {
				final IDdata id = readIDdata(
						child,
						xmlns);
				trackManagement.stationId = id.getStationId();
				trackManagement.nationality = id.getNationality();
			}
			else if ("trackerType".equals(childName)) {
				try {
					trackManagement.trackerType = TrackerType.valueOf(childValue);
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set tracker type",
							iae);
					trackManagement.trackerType = null;
				}
			}
			else if ("alertIndicator".equals(childName)) {
				try {
					trackManagement.alertIndicator = Boolean.parseBoolean(childValue);
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set alert indicator",
							iae);
					trackManagement.alertIndicator = false;
				}
			}
		}
		return trackManagement;
	}

	private MotionImagery readMotionImagery(
			final Element element,
			final Namespace xmlns ) {
		final MotionImagery motionImagery = new MotionImagery();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("trackItemUUID".equals(childName)) {
				try {
					motionImagery.setUuid(UUID.fromString(childValue));
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set uuid",
							iae);
					motionImagery.setUuid(null);
				}
			}
			else if ("trackItemSecurity".equals(childName)) {
				motionImagery.setSecurity(readSecurity(
						child,
						xmlns));
			}
			else if ("trackItemTime".equals(childName)) {
				motionImagery.setTime(DateStringToLong(childValue));
			}
			else if ("trackItemSource".equals(childName)) {
				motionImagery.setSource(childValue);
			}
			else if ("trackItemComment".equals(childName)) {
				motionImagery.setComment(childValue);
			}
			else if ("band".equals(childName)) {
				try {
					motionImagery.band = SymbolicSpectralRange.valueOf(childValue);
				}
				catch (final IllegalArgumentException iae) {
					LOGGER.warn(
							"Unable to set band value",
							iae);
					motionImagery.band = null;
				}
			}
			else if ("imageReference".equals(childName)) {
				motionImagery.imageReference = childValue;
			}
			else if ("imageChip".equals(childName)) {
				motionImagery.imageChip = child.getText();
			}
			else if ("frameNumber".equals(childName)) {
				motionImagery.frameNumber = Integer.parseInt(child.getText());
			}
			else if ("pixelRow".equals(childName)) {
				motionImagery.pixelRow = Integer.parseInt(child.getText());
			}
			else if ("pixelColumn".equals(childName)) {
				motionImagery.pixelColumn = Integer.parseInt(child.getText());
			}
		}
		return motionImagery;
	}

	private LineageRelation readLineageRelation(
			final Element element,
			final Namespace xmlns ) {
		final LineageRelation relation = new LineageRelation();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("relations".equals(childName)) {
				// TODO: TrackLineageInformation / LineageRelation
			}
		}
		return relation;
	}

	private ClassificationCredibility readClassificationCredibility(
			final Element element,
			final Namespace xmlns ) {
		final ClassificationCredibility credibility = new ClassificationCredibility();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("valueConfidence".equals(childName)) {
				try {
					credibility.setValueConfidence(Integer.parseInt(childValue));
				}
				catch (final NumberFormatException nfe) {}
			}
			else if ("sourceReliability".equals(childName)) {
				try {
					credibility.setSourceReliability(Integer.parseInt(childValue));
				}
				catch (final NumberFormatException nfe) {}
			}
		}
		return credibility;
	}

	private GeodeticPosition readGeodeticPosition(
			final Element element,
			final Namespace xmlns ) {
		final GeodeticPosition pos = new GeodeticPosition();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("latitude".equals(childName)) {
				try {
					pos.latitude = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					pos.latitude = null;
				}
			}
			else if ("longitude".equals(childName)) {
				try {
					pos.longitude = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					pos.longitude = null;
				}
			}
			else if ("elevation".equals(childName)) {
				try {
					pos.elevation = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					pos.elevation = null;
				}
			}
		}
		return pos;
	}

	private Area readArea(
			final Element element,
			final Namespace xmlns ) {
		final Area area = new Area();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("xxx".equals(childName)) {
				// area.setXXX(childValue);
				// TODO: Area , CircularArea, PolygonArea, etc...
			}
		}
		return area;
	}

	private Area readCoverageArea(
			final Element element,
			final Namespace xmlns ) {
		final Area area = new Area();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("areaBoundaryPoints".equals(childName)) {
				GeodeticPosition pos = readGeodeticPosition(
						child,
						xmlns);
				area.getPoints().add(
						pos);
			}
		}
		return area;
	}

	private TrackPointDetail readTrackPointDetail(
			final Element element,
			final Namespace xmlns ) {
		final Namespace xsi = Namespace.getNamespace(
				"xsi",
				"http://www.w3.org/2001/XMLSchema-instance");
		final TrackPointDetail detail = new TrackPointDetail();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("pointDetailPosition".equals(childName)) {
				// check which type...
				final Attribute xsitype = child.getAttribute(
						"type",
						xsi);
				if (xsitype != null) {
					if ("GeodeticPosition".equals(xsitype.getValue())) {
						detail.location = readGeodeticPosition(
								child,
								xmlns);
					}
					else if ("LocalCartesianPosition".equals(xsitype.getValue())) {
						// TODO: Add support for reading LocalCartesianPosition
					}
				}
				else {
					try {
						final GeodeticPosition geoPos = readGeodeticPosition(
								child,
								xmlns);
						if (geoPos != null) {
							detail.location = geoPos;
						}
					}
					catch (final Exception e) {
						LOGGER.error(
								"Could not identify TrackPoint position type",
								e);
					}
				}
			}
			else if ("pointDetailVelocity".equals(childName)) {
				final Attribute xsitype = child.getAttribute(
						"type",
						xsi);
				if (xsitype != null) {
					if ("LocalCartesianVelocity".equals(xsitype.getValue())) {
						final Double[] velCoords = readLocalCartesianVelocity(child);
						detail.velocityX = velCoords[0];
						detail.velocityY = velCoords[1];
						detail.velocityZ = velCoords[2];
					}
				}
				else {
					try {
						final Double[] velCoords = readLocalCartesianVelocity(child);
						detail.velocityX = velCoords[0];
						detail.velocityY = velCoords[1];
						detail.velocityZ = velCoords[2];
					}
					catch (final Exception e) {
						LOGGER.error(
								"Could not identify TrackPoint velocity",
								e);
					}
				}
			}
			else if ("pointDetailAcceleration".equals(childName)) {
				final Attribute xsitype = child.getAttribute(
						"type",
						xsi);
				if (xsitype != null) {
					if ("LocalCartesianAcceleration".equals(xsitype.getValue())) {
						final Double[] accelCoords = readLocalCartesianAccel(child);
						detail.accelerationX = accelCoords[0];
						detail.accelerationY = accelCoords[1];
						detail.accelerationZ = accelCoords[2];
					}
				}
				else {
					try {
						final Double[] accelCoords = readLocalCartesianAccel(child);
						detail.accelerationX = accelCoords[0];
						detail.accelerationY = accelCoords[1];
						detail.accelerationZ = accelCoords[2];
					}
					catch (final Exception e) {
						LOGGER.error(
								"Could not identify TrackPoint velocity",
								e);
					}
				}
			}
			else if ("pointDetailCovarianceMatrix".equals(childName)) {
				detail.covarianceMatrix = readCovarianceMatrix(
						child,
						xmlns);
			}
		}
		return detail;
	}

	private Double[] readLocalCartesianVelocity(
			final Element child ) {
		final Double[] velCoords = new Double[3];

		final List<Element> grandchildren = child.getChildren();
		final Iterator<Element> grandchildIter = grandchildren.iterator();
		while (grandchildIter.hasNext()) {
			final Element grandchild = grandchildIter.next();
			final String grandchildName = grandchild.getName();
			final String grandchildValue = grandchild.getValue();
			if ("velx".equals(grandchildName)) {
				try {
					velCoords[0] = Double.parseDouble(grandchildValue);
				}
				catch (final NumberFormatException nfe) {
					velCoords[0] = null;
				}
			}
			else if ("vely".equals(grandchildName)) {
				try {
					velCoords[1] = Double.parseDouble(grandchildValue);
				}
				catch (final NumberFormatException nfe) {
					velCoords[1] = null;
				}
			}
			else if ("velz".equals(grandchildName)) {
				try {
					velCoords[2] = Double.parseDouble(grandchildValue);
				}
				catch (final NumberFormatException nfe) {
					velCoords[2] = null;
				}
			}
		}

		return velCoords;
	}

	private Double[] readLocalCartesianAccel(
			final Element child ) {
		final Double[] accelCoords = new Double[3];

		final List<Element> grandchildren = child.getChildren();
		final Iterator<Element> grandchildIter = grandchildren.iterator();
		while (grandchildIter.hasNext()) {
			final Element grandchild = grandchildIter.next();
			final String grandchildName = grandchild.getName();
			final String grandchildValue = grandchild.getValue();
			if ("accx".equals(grandchildName)) {
				try {
					accelCoords[0] = Double.parseDouble(grandchildValue);
				}
				catch (final NumberFormatException nfe) {
					accelCoords[0] = null;
				}
			}
			else if ("accy".equals(grandchildName)) {
				try {
					accelCoords[1] = Double.parseDouble(grandchildValue);
				}
				catch (final NumberFormatException nfe) {
					accelCoords[1] = null;
				}
			}
			else if ("accz".equals(grandchildName)) {
				try {
					accelCoords[2] = Double.parseDouble(grandchildValue);
				}
				catch (final NumberFormatException nfe) {
					accelCoords[2] = null;
				}
			}
		}

		return accelCoords;
	}

	private CovarianceMatrix readCovarianceMatrix(
			final Element element,
			final Namespace xmlns ) {
		final CovarianceMatrix matrix = new CovarianceMatrix();
		final List<Element> children = element.getChildren();
		final Iterator<Element> childIter = children.iterator();
		while (childIter.hasNext()) {
			final Element child = childIter.next();
			final String childName = child.getName();
			final String childValue = child.getValue();
			if ("covPosxPosx".equals(childName)) {
				try {
					matrix.covPosXPosX = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosXPosX = null;
				}
			}
			else if ("covPosyPosy".equals(childName)) {
				try {
					matrix.covPosYPosY = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosYPosY = null;
				}
			}
			else if ("covPoszPosz".equals(childName)) {
				try {
					matrix.covPosZPosZ = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosZPosZ = null;
				}
			}
			else if ("covPosxPosy".equals(childName)) {
				try {
					matrix.covPosXPosY = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosXPosY = null;
				}
			}
			else if ("covPosxPosz".equals(childName)) {
				try {
					matrix.covPosXPosZ = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosXPosZ = null;
				}
			}
			else if ("covPosyPosz".equals(childName)) {
				try {
					matrix.covPosYPosZ = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosYPosZ = null;
				}
			}
			else if ("covVelxVelx".equals(childName)) {
				try {
					matrix.covVelXVelX = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covVelXVelX = null;
				}
			}
			else if ("covVelyVely".equals(childName)) {
				try {
					matrix.covVelYVelY = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covVelYVelY = null;
				}
			}
			else if ("covVelzVelz".equals(childName)) {
				try {
					matrix.covVelZVelZ = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covVelZVelZ = null;
				}
			}
			else if ("covPosxVelx".equals(childName)) {
				try {
					matrix.covPosXVelX = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosXVelX = null;
				}
			}
			else if ("covPosxVely".equals(childName)) {
				try {
					matrix.covPosXVelY = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosXVelY = null;
				}
			}
			else if ("covPosxVelz".equals(childName)) {
				try {
					matrix.covPosXVelZ = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosXVelZ = null;
				}
			}
			else if ("covPosyVelx".equals(childName)) {
				try {
					matrix.covPosYVelX = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosYVelX = null;
				}
			}
			else if ("covPosyVely".equals(childName)) {
				try {
					matrix.covPosYVelY = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosYVelY = null;
				}
			}
			else if ("covPosyVelz".equals(childName)) {
				try {
					matrix.covPosYVelZ = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosYVelZ = null;
				}
			}
			else if ("covPoszVelx".equals(childName)) {
				try {
					matrix.covPosZVelX = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosZVelX = null;
				}
			}
			else if ("covPoszVely".equals(childName)) {
				try {
					matrix.covPosZVelY = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosZVelY = null;
				}
			}
			else if ("covPoszVelz".equals(childName)) {
				try {
					matrix.covPosZVelZ = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covPosZVelZ = null;
				}
			}
			else if ("covVelxVely".equals(childName)) {
				try {
					matrix.covVelXVelY = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covVelXVelY = null;
				}
			}
			else if ("covVelxVelz".equals(childName)) {
				try {
					matrix.covVelXVelZ = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covVelXVelZ = null;
				}
			}
			else if ("covVelyVelz".equals(childName)) {
				try {
					matrix.covVelYVelZ = Double.parseDouble(childValue);
				}
				catch (final NumberFormatException nfe) {
					matrix.covVelYVelZ = null;
				}
			}
		}
		return matrix;
	}

	private long DateStringToLong(
			String dateString ) {

		// matches on ".??????Z" and removes the last three ??? (microseconds)
		dateString = dateString.replaceAll(
				"(\\.\\d{3})\\d+[Z]",
				"$1Z");
		Date date = parseHelper(
				dateString,
				"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		if (date == null) {
			date = parseHelper(
					dateString,
					"yyyy-MM-dd'T'HH:mm:ss.SSS");
			if (date == null) {
				date = parseHelper(
						dateString,
						"yyyy-MM-dd'T'HH:mm:ss'Z'");
				if (date == null) {
					date = parseHelper(
							dateString,
							"yyyy-MM-dd'T'HH:mm:ss");
					if (date == null) {
						return 0;
					}
				}
			}
		}
		return date.getTime();
	}

	private Date parseHelper(
			final String dateString,
			final String format ) {
		final DateFormat parser = new SimpleDateFormat(
				format);
		parser.setTimeZone(TimeZone.getTimeZone("GMT"));
		try {
			return parser.parse(dateString);
		}
		catch (final ParseException e) {
			return null;
		}
	}

}
