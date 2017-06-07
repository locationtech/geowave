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

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;

import mil.nga.giat.geowave.format.stanag4676.parser.model.Area;
import mil.nga.giat.geowave.format.stanag4676.parser.model.ClassificationCredibility;
import mil.nga.giat.geowave.format.stanag4676.parser.model.ClassificationLevel;
import mil.nga.giat.geowave.format.stanag4676.parser.model.CovarianceMatrix;
import mil.nga.giat.geowave.format.stanag4676.parser.model.ExerciseIndicator;
import mil.nga.giat.geowave.format.stanag4676.parser.model.GeodeticPosition;
import mil.nga.giat.geowave.format.stanag4676.parser.model.IDdata;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MissionFrame;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MissionSummary;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MissionSummaryMessage;
import mil.nga.giat.geowave.format.stanag4676.parser.model.ModalityType;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MotionImagery;
import mil.nga.giat.geowave.format.stanag4676.parser.model.NATO4676Message;
import mil.nga.giat.geowave.format.stanag4676.parser.model.ObjectClassification;
import mil.nga.giat.geowave.format.stanag4676.parser.model.Security;
import mil.nga.giat.geowave.format.stanag4676.parser.model.SimulationIndicator;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackClassification;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackEvent;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackIdentity;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackManagement;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackMessage;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackPoint;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackPointDetail;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackRun;

public class NATO4676Encoder implements
		TrackEncoder
{
	private static final Charset UTF_8 = Charset.forName("UTF-8");
	private final ExerciseIndicator defaultExerciseIndicator;
	private final Security defaultSecurity;
	private SimulationIndicator defaultSimulationIndicator;
	private int indentLevel = 0;
	private final String stanagVersion;
	private PrintWriter pw = null;
	private OutputStream trackOut = null;
	private OutputStream missionOut = null;

	private PrintWriter pwTrack = null;
	private PrintWriter pwMission = null;

	private String indent() {
		if (indentLevel == 0) {
			return "";
		}
		final char[] indention = new char[indentLevel];
		Arrays.fill(
				indention,
				'\t');
		return new String(
				indention);
	}

	public void setDefaultSecurityLevel(
			final String level ) {
		defaultSecurity.setClassification(ClassificationLevel.valueOf(level));
	}

	public void setDefaultSimulationString(
			final String simString ) {
		defaultSimulationIndicator = SimulationIndicator.valueOf(simString);
	}

	public NATO4676Encoder() {
		defaultSecurity = new Security();
		defaultSecurity.setClassification(ClassificationLevel.UNCLASSIFIED);
		defaultSecurity.setPolicyName("NATO");
		defaultExerciseIndicator = ExerciseIndicator.OPERATIONAL;
		stanagVersion = "1.0";
	}

	@Override
	public void setOutputStreams(
			final OutputStream trackOut,
			final OutputStream missionOut ) {

		this.trackOut = trackOut;
		this.missionOut = missionOut;
		final OutputStreamWriter trackOsw = new OutputStreamWriter(
				this.trackOut,
				UTF_8);
		final OutputStreamWriter missionOsw = new OutputStreamWriter(
				this.missionOut,
				UTF_8);

		pwTrack = new PrintWriter(
				new BufferedWriter(
						trackOsw,
						8192));

		pw = pwTrack;

		pwMission = new PrintWriter(
				new BufferedWriter(
						missionOsw,
						8192));
	}

	private String GetXMLOpen() {
		return "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n";
	}

	private String GetXMLClose() {
		return "";
	}

	/**
	 * A TrackRun will be encoded as a single NATO4676Message even though there
	 * may be multiple messages inside it. The LAST NATO4676Message should be
	 * used.
	 * 
	 * 
	 * @param run
	 * @return
	 */
	@Override
	public void Encode(
			final TrackRun run ) {
		boolean firstTrackMessage = true;
		boolean trackMessagesExist = false;
		for (final NATO4676Message msg : run.getMessages()) {
			indentLevel = 0;
			if (msg instanceof TrackMessage) {
				TrackMessage trackMsg = (TrackMessage) msg;
				if (firstTrackMessage) {
					pw.write(GetXMLOpen());
					pw
							.write("<TrackMessage xmlns=\"urn:int:nato:stanag4676:0.14\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" schemaVersion=\"0.14\">\n");
					indentLevel++;
					EncodeMsgMetadata(msg);
					indentLevel--;
					firstTrackMessage = false;
					trackMessagesExist = true;
				}
				Encode(trackMsg);
			}
			else if (msg instanceof MissionSummaryMessage) {
				pw = pwMission;
				MissionSummaryMessage msMsg = (MissionSummaryMessage) msg;
				MissionSummary ms = msMsg.getMissionSummary();
				if (ms != null) {
					pw.write(GetXMLOpen());
					pw
							.write("<MissionSummary xmlns=\"http://siginnovations.com/MissionSummarySIG\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n");
					indentLevel++;
					EncodeMsgMetadata(msg);
					pw.write(indent() + "<Name>" + ms.getName() + "</Name>\n");
					pw.write(indent() + "<missionID>" + ms.getMissionId() + "</missionID>\n");
					pw.write(indent() + "<StartTime>" + EncodeTime(ms.getStartTime()) + "</StartTime>\n");
					pw.write(indent() + "<EndTime>" + EncodeTime(ms.getEndTime()) + "</EndTime>\n");
					Area area = ms.getCoverageArea();
					if (area != null) {
						pw.write(indent() + "<CoverageArea xsi:type=\"PolygonArea\">\n");
						Encode(area);
						pw.write(indent() + "</CoverageArea>\n");
					}
					if (ms.getClassifications().size() > 0) {
						pw.write(indent() + "<ActiveObjectClassifications>\n");
						indentLevel++;
						for (ObjectClassification oc : ms.getClassifications()) {
							pw.write(indent() + "<classification>" + oc.name() + "</classification>\n");
						}
						indentLevel--;
						pw.write(indent() + "</ActiveObjectClassifications>\n");
					}
					indentLevel--;
					Encode(msMsg);
					pw.write("</MissionSummary>\n");
					pw.flush();
					pw = pwTrack;
				}
			}
		}
		if (trackMessagesExist) {
			pw.write("</TrackMessage>\n");
			pw.flush();
		}
	}

	private void Encode(
			final TrackMessage msg ) {
		indentLevel++;
		for (final TrackEvent trackevent : msg.getTracks()) {
			pw.write(indent() + "<tracks>\n");
			Encode(trackevent);
			pw.write(indent() + "</tracks>\n");
		}
		indentLevel--;
	}

	private void Encode(
			final MissionSummaryMessage msg ) {
		indentLevel++;
		for (final MissionFrame frame : msg.getMissionSummary().getFrames()) {
			pw.write(indent() + "<FrameInformation>\n");
			Encode(frame);
			pw.write(indent() + "</FrameInformation>\n");
		}
		indentLevel--;
	}

	private void EncodeMsgMetadata(
			final NATO4676Message msg ) {
		pw.write(indent() + "<stanagVersion>" + stanagVersion + "</stanagVersion>\n");
		pw.write(indent() + "<messageSecurity>\n");
		Encode(msg.getSecurity());
		pw.write(indent() + "</messageSecurity>\n");
		pw.write(indent() + "<msgCreatedTime>" + EncodeTime(msg.getMessageTime()) + "</msgCreatedTime>\n");
		pw.write(indent() + "<senderId>\n");
		Encode(msg.getSenderID());
		pw.write(indent() + "</senderId>\n");
	}

	private void Encode(
			Security sec ) {
		if (sec == null) {
			sec = defaultSecurity;
		}
		indentLevel++;
		pw.write(indent() + "<securityClassification>" + sec.getClassification() + "</securityClassification>\n");
		pw.write(indent() + "<securityPolicyName>" + sec.getPolicyName() + "</securityPolicyName>\n");
		if (sec.getControlSystem() != null) {
			pw.write(indent() + "<securityControlSystem>" + sec.getControlSystem() + "</securityControlSystem>\n");
		}
		if (sec.getDissemination() != null) {
			pw.write(indent() + "<securityDissemination>" + sec.getDissemination() + "</securityDissemination>\n");
		}
		if (sec.getReleasability() != null) {
			pw.write(indent() + "<securityReleasability>" + sec.getReleasability() + "</securityReleasability>\n");
		}
		indentLevel--;
	}

	private void Encode(
			final IDdata id ) {
		indentLevel++;
		pw.write(indent() + "<stationID>" + id.getStationId() + "</stationID>\n");
		pw.write(indent() + "<nationality>" + id.getNationality() + "</nationality>\n");
		indentLevel--;
	}

	private void Encode(
			final TrackEvent event ) {
		indentLevel++;
		pw.write(indent() + "<trackUUID>" + event.getUuid() + "</trackUUID>\n");
		pw.write(indent() + "<trackNumber>" + event.getTrackNumber() + "</trackNumber>\n");
		if (event.getStatus() != null) {
			pw.write(indent() + "<trackStatus>" + event.getStatus() + "</trackStatus>\n");
		}
		pw.write(indent() + "<trackSecurity>\n");
		Encode(event.getSecurity());
		pw.write(indent() + "</trackSecurity>\n");
		if (event.getComment() != null) {
			pw.write(indent() + "<trackComment>" + event.getComment() + "</trackComment>\n");
		}
		if (event.getMissionId() != null) {
			pw.write(indent() + "<missionID>" + event.getMissionId() + "</missionID>\n");
		}
		if (event.getExerciseIndicator() != null) {
			pw.write(indent() + "<exerciseIndicator>" + event.getExerciseIndicator() + "</exerciseIndicator>\n");
		}
		else {
			pw.write(indent() + "<exerciseIndicator>" + defaultExerciseIndicator + "</exerciseIndicator>\n");
		}
		if (event.getSimulationIndicator() != null) {
			pw.write(indent() + "<simulationIndicator>" + event.getSimulationIndicator() + "</simulationIndicator>\n");
		}
		else {
			pw.write(indent() + "<simulationIndicator>" + defaultSimulationIndicator + "</simulationIndicator>\n");
		}
		for (final TrackPoint point : event.getPoints().values()) {
			pw.write(indent() + "<items xsi:type=\"TrackPoint\">\n");
			Encode(point);
			pw.write(indent() + "</items>\n");
		}
		for (final TrackIdentity identity : event.getIdentities()) {
			pw.write(indent() + "<items xsi:type=\"TrackIdentityInformation\">\n");
			Encode(identity);
			pw.write(indent() + "</items>\n");
		}
		for (final TrackClassification tc : event.getClassifications()) {
			pw.write(indent() + "<items xsi:type=\"TrackClassificationInformation\">\n");
			Encode(tc);
			pw.write(indent() + "</items>\n");
		}
		for (final TrackManagement management : event.getManagements()) {
			pw.write(indent() + "<items xsi:type=\"TrackManagementInformation\">\n");
			Encode(management);
			pw.write(indent() + "</items>\n");
		}

		for (final MotionImagery image : event.getMotionImages()) {
			pw.write(indent() + "<items xsi:type=\"MotionImageryInformation\">\n");
			Encode(image);
			pw.write(indent() + "</items>\n");
		}
		// TODO: ESMInformation
		// TODO: TrackLineageInformation
		indentLevel--;
	}

	private void Encode(
			final TrackPoint point ) {
		indentLevel++;
		pw.write(indent() + "<trackItemUUID>" + point.getUuid() + "</trackItemUUID>\n");

		pw.write(indent() + "<trackItemSecurity>\n");
		Encode(point.getSecurity());
		pw.write(indent() + "</trackItemSecurity>\n");

		pw.write(indent() + "<trackItemTime>\n");
		pw.write(EncodeTime(point.getEventTime()));
		pw.write(indent() + "</trackItemTime>\n");

		if (point.getTrackItemSource() != null) {
			pw.write(indent() + "<trackItemSource>" + point.getTrackItemSource() + "</trackItemSource>\n");
		}
		if (point.getTrackItemComment() != null) {
			pw.write(indent() + "<trackItemComment>" + point.getTrackItemComment() + "</trackItemComment>\n");
		}

		pw.write(indent() + "<trackPointPosition>\n");
		Encode(point.getLocation());
		pw.write(indent() + "</trackPointPosition>\n");

		if (point.getSpeed() != null) {
			pw.write(indent() + "<trackPointSpeed>" + point.getSpeed().intValue() + "</trackPointSpeed>\n");
		}
		if (point.getCourse() != null) {
			pw.write(indent() + "<trackPointCourse>" + point.getCourse() + "</trackPointCourse>\n");
		}
		if (point.getTrackPointType() != null) {
			pw.write(indent() + "<trackPointType>" + point.getTrackPointType() + "</trackPointType>\n");
		}
		if (point.getTrackPointSource() != null) {
			pw.write(indent() + "<trackPointSource>" + point.getTrackPointSource() + "</trackPointSource>\n");
		}
		// TODO: need objectMask here
		if (point.getDetail() != null) {
			pw.write(indent() + "<TrackPointDetail>\n");
			Encode(point.getDetail());
			pw.write(indent() + "</TrackPointDetail>\n");
		}
		indentLevel--;
	}

	private void Encode(
			final MissionFrame frame ) {
		indentLevel++;
		pw.write(indent() + "<frameNumber>" + frame.getFrameNumber() + "</frameNumber>\n");
		pw.write(indent() + "<frameTimestamp>" + EncodeTime(frame.getFrameTime()) + "</frameTimestamp>\n");
		Area area = frame.getCoverageArea();
		if (area != null) {
			pw.write(indent() + "<frameCoverageArea xsi:type=\"PolygonArea\">\n");
			Encode(frame.getCoverageArea());
			pw.write(indent() + "</frameCoverageArea>\n");
		}
		pw.write(indent() + "<hasFault>false</hasFault>\n");
		indentLevel--;
	}

	private void Encode(
			final TrackIdentity identity ) {
		// TODO: Encode TrackIdentity
	}

	private void Encode(
			final TrackClassification tc ) {
		indentLevel++;
		pw.write(indent() + "<trackItemUUID>" + tc.getUuid() + "</trackItemUUID>\n");

		pw.write(indent() + "<trackItemSecurity>\n");
		Encode(tc.getSecurity());
		pw.write(indent() + "</trackItemSecurity>\n");

		pw.write(indent() + "<trackItemTime>" + EncodeTime(tc.getTime()) + "</trackItemTime>\n");
		pw.write(indent() + "<numberofObjects>" + tc.getNumObjects() + "</numberofObjects>\n");

		ObjectClassification oc = tc.getClassification();
		if (oc != null) {
			pw.write(indent() + "<classification>" + oc.name() + "</classification>\n");
			ModalityType mt = ModalityType.fromString(tc.getSource());
			if (mt != null) {
				pw.write(indent() + "<classificationSource>" + mt.toString() + "</classificationSource>\n");
			}
		}
		ClassificationCredibility cred = tc.getCredibility();
		if (cred != null) {
			pw.write(indent() + "<classificationCredibility>\n");
			indentLevel++;
			pw.write(indent() + "<valueConfidence>" + cred.getValueConfidence() + "</valueConfidence>\n");
			pw.write(indent() + "<sourceReliability>" + cred.getSourceReliability() + "</sourceReliability>\n");
			indentLevel--;
			pw.write(indent() + "</classificationCredibility>\n");
		}
		indentLevel--;
	}

	private void Encode(
			final TrackManagement management ) {
		// TODO: Encode TrackManagement
	}

	private void Encode(
			final MotionImagery image ) {
		pw.write("\n");
		indentLevel++;
		if (image.getBand() != null) {
			pw.write(indent() + "<band>" + image.getBand().toString() + "</band>\n");
		}
		if (image.getImageReference() != null) {
			pw.write(indent() + "<imageReference>" + image.getImageReference() + "</imageReference>\n");
		}
		if (image.getImageChip() != null) {
			pw.write(indent() + "<imageChip>\n");
			EncodeImage(image.getImageChip());
			pw.write(indent() + "</imageChip>\n");
		}
		indentLevel--;
	}

	private void EncodeImage(
			final String base64imageChip ) {
		indentLevel++;
		pw.write(indent() + "<![CDATA[" + base64imageChip + "]]>\n");
		indentLevel--;
	}

	private String EncodeTime(
			final Long time ) {
		// change long to 2011-08-24T18:05:30.375Z format
		final SimpleDateFormat sdf = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		final String xml = sdf.format(new Date(
				time));
		return xml;
	}

	private void Encode(
			final GeodeticPosition pos ) {
		indentLevel++;
		pw.write(indent() + "<latitude>" + pos.latitude + "</latitude>\n");
		pw.write(indent() + "<longitude>" + pos.longitude + "</longitude>\n");
		if (pos.elevation != null) {
			pw.write(indent() + "<elevation>" + pos.elevation + "</elevation>\n");
		}
		indentLevel--;
	}

	private void Encode(
			final Area coverageArea ) {
		indentLevel++;
		if (coverageArea != null) {
			for (GeodeticPosition pos : coverageArea.getPoints()) {
				pw.write(indent() + "<areaBoundaryPoints xsi:type=\"GeodeticPosition\">\n");
				Encode(pos);
				pw.write(indent() + "</areaBoundaryPoints>\n");
			}
		}
		indentLevel--;
	}

	private void Encode(
			final TrackPointDetail detail ) {
		indentLevel++;
		pw.write(indent() + "<pointDetailPosition xsi:type=\"GeodeticPosition\">\n");
		Encode(detail.getLocation());
		pw.write(indent() + "</pointDetailPosition>\n");

		if ((detail.getVelocityX() != null) || (detail.getVelocityY() != null) || (detail.getVelocityZ() != null)) {
			pw.write(indent() + "<pointDetailVelocity xsi:type=\"LocalCartesianVelocity\">\n");
			indentLevel++;
			if (detail.getVelocityX() != null) {
				pw.write(indent() + "<velx>" + detail.getVelocityX() + "</velx>\n");
			}
			else {
				pw.write(indent() + "<velx>0</velx>\n");
			}
			if (detail.getVelocityY() != null) {
				pw.write(indent() + "<vely>" + detail.getVelocityY() + "</vely>\n");
			}
			else {
				pw.write(indent() + "<vely>0</vely>\n");
			}
			if (detail.getVelocityZ() != null) {
				pw.write(indent() + "<velz>" + detail.getVelocityZ() + "</velz>\n");
			}
			else {
				pw.write(indent() + "<velz>0</velz>\n");
			}
			indentLevel--;
			pw.write(indent() + "</pointDetailVelocity>\n");
		}
		if ((detail.getAccelerationX() != null) || (detail.getAccelerationY() != null)
				|| (detail.getAccelerationZ() != null)) {
			pw.write(indent() + "<pointDetailAcceleration xsi:type=\"LocalCartesianAcceleration\">\n");
			indentLevel++;
			if (detail.getAccelerationX() != null) {
				pw.write(indent() + "<accx>" + detail.getAccelerationX() + "</accx>\n");
			}
			else {
				pw.write(indent() + "<accx>0</accx>\n");
			}
			if (detail.getAccelerationY() != null) {
				pw.write(indent() + "<accy>" + detail.getAccelerationY() + "</accy>\n");
			}
			else {
				pw.write(indent() + "<accy>0</accy>\n");
			}
			if (detail.getAccelerationZ() != null) {
				pw.write(indent() + "<accz>" + detail.getAccelerationZ() + "</accz>\n");
			}
			else {
				pw.write(indent() + "<accz>0</accz>\n");
			}
			indentLevel--;
			pw.write(indent() + "</pointDetailAcceleration>\n");
		}
		pw.write(indent() + "<pointDetailCovarianceMatrix xsi:type=\"CovarianceMatrixPositionVelocity\">");
		Encode(detail.getCovarianceMatrix());
		pw.write(indent() + "</pointDetailCovarianceMatrix>\n");
		indentLevel--;
	}

	private void Encode(
			final CovarianceMatrix cov ) {
		indentLevel++;
		pw.write(indent() + "<covPosxPosx>" + cov.getCovPosXPosX() + "</covPosxPosx>\n");
		pw.write(indent() + "<covPosyPosy>" + cov.getCovPosYPosY() + "</covPosyPosy>\n");
		if (cov.getCovPosZPosZ() != null) {
			pw.write(indent() + "<covPoszPosz>" + cov.getCovPosZPosZ() + "</covPoszPosz>\n");
		}
		if (cov.getCovPosXPosY() != null) {
			pw.write(indent() + "<covPosxPosy>" + cov.getCovPosXPosY() + "</covPosxPosy>\n");
		}
		if (cov.getCovPosXPosZ() != null) {
			pw.write(indent() + "<covPosxPosz>" + cov.getCovPosXPosZ() + "</covPosxPosz>\n");
		}
		if (cov.getCovPosYPosZ() != null) {
			pw.write(indent() + "<covPosyPosz>" + cov.getCovPosYPosZ() + "</covPosyPosz>\n");
		}
		// these are also optional
		if (cov.getCovVelXVelX() != null) {
			pw.write(indent() + "<covVelxVelx>" + cov.getCovVelXVelX() + "</covVelxVelx>\n");
		}
		if (cov.getCovVelYVelY() != null) {
			pw.write(indent() + "<covVelyVely>" + cov.getCovVelYVelY() + "</covVelyVely>\n");
		}
		//
		if (cov.getCovVelZVelZ() != null) {
			pw.write(indent() + "<covVelzVelz>" + cov.getCovVelZVelZ() + "</covVelzVelz>\n");
		}
		if (cov.getCovPosXVelX() != null) {
			pw.write(indent() + "<covPosxVelx>" + cov.getCovPosXVelX() + "</covPosxVelx>\n");
		}
		if (cov.getCovPosXVelY() != null) {
			pw.write(indent() + "<covPosxVely>" + cov.getCovPosXVelY() + "</covPosxVely>\n");
		}
		if (cov.getCovPosXVelZ() != null) {
			pw.write(indent() + "<covPosxVelz>" + cov.getCovPosXVelZ() + "</covPosxVelz>\n");
		}
		if (cov.getCovPosYVelX() != null) {
			pw.write(indent() + "<covPosyVelx>" + cov.getCovPosYVelX() + "</covPosyVelx>\n");
		}
		if (cov.getCovPosYVelY() != null) {
			pw.write(indent() + "<covPosyVely>" + cov.getCovPosYVelY() + "</covPosyVely>\n");
		}
		if (cov.getCovPosYVelZ() != null) {
			pw.write(indent() + "<covPosyVelz>" + cov.getCovPosYVelZ() + "</covPosyVelz>\n");
		}
		if (cov.getCovPosZVelX() != null) {
			pw.write(indent() + "<covPoszVelx>" + cov.getCovPosZVelX() + "</covPoszVelx>\n");
		}
		if (cov.getCovPosZVelY() != null) {
			pw.write(indent() + "<covPoszVely>" + cov.getCovPosZVelY() + "</covPoszVely>\n");
		}
		if (cov.getCovPosZVelZ() != null) {
			pw.write(indent() + "<covPoszVelz>" + cov.getCovPosZVelZ() + "</covPoszVelz>\n");
		}
		if (cov.getCovVelXVelY() != null) {
			pw.write(indent() + "<covVelxVely>" + cov.getCovVelXVelY() + "</covVelxVely>\n");
		}
		if (cov.getCovVelXVelZ() != null) {
			pw.write(indent() + "<covVelxVelz>" + cov.getCovVelXVelZ() + "</covVelxVelz>\n");
		}
		if (cov.getCovVelYVelZ() != null) {
			pw.write(indent() + "<covVelyVelz>" + cov.getCovVelYVelZ() + "</covVelyVelz>\n");
		}
		indentLevel--;
	}
}
