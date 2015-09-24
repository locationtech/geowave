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

import mil.nga.giat.geowave.format.stanag4676.parser.model.ClassificationLevel;
import mil.nga.giat.geowave.format.stanag4676.parser.model.CovarianceMatrix;
import mil.nga.giat.geowave.format.stanag4676.parser.model.ExerciseIndicator;
import mil.nga.giat.geowave.format.stanag4676.parser.model.GeodeticPosition;
import mil.nga.giat.geowave.format.stanag4676.parser.model.IDdata;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MotionImagery;
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
	private OutputStream out = null;
	private PrintWriter printout = null;

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
	public void setOutputStream(
			final OutputStream os ) {
		out = os;
		final OutputStreamWriter osw = new OutputStreamWriter(
				os,
				UTF_8);
		printout = new PrintWriter(
				new BufferedWriter(
						osw,
						8192));
	}

	private String GetXMLOpen() {
		return "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n";
	}

	private String GetXMLClose() {
		return "";
	}

	/**
	 * A TrackRun will be encoded as a single TrackMessage even though there may
	 * be multiple messages inside it. The LAST TrackMessage should be used.
	 * 
	 * 
	 * @param run
	 * @return
	 */
	@Override
	public void Encode(
			final TrackRun run ) {
		// make sure no one interrupts your stream
		synchronized (out) {
			boolean firstMessage = true;
			printout.write(GetXMLOpen());
			printout.write("<TrackMessage xmlns=\"urn:int:nato:stanag4676:0.14\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" schemaVersion=\"0.14\">\n");
			indentLevel++;
			for (final TrackMessage msg : run.getMessages()) {
				if (firstMessage) {
					printout.write(indent() + "<stanagVersion>" + stanagVersion + "</stanagVersion>\n");

					printout.write(indent() + "<messageSecurity>");
					Encode(msg.getSecurity());
					printout.write("</messageSecurity>\n");

					printout.write(indent() + "<msgCreatedTime>" + EncodeTime(msg.getMessageTime()) + "</msgCreatedTime>\n");

					printout.write(indent() + "<senderId>");
					Encode(msg.getSenderID());
					printout.write("</senderId>\n");

					firstMessage = false;
				}
				for (final TrackEvent trackevent : msg.getTracks()) {
					printout.write(indent() + "<tracks>");
					Encode(trackevent);
					printout.write("</tracks>\n");
				}
			}
			indentLevel--;
			printout.write("</TrackMessage>\n");
			printout.flush();
		}
	}

	@Override
	public void Encode(
			final TrackMessage msg ) {
		// make sure no one interrupts your stream
		synchronized (out) {
			printout.write(GetXMLOpen());
			printout.write("<TrackMessage xmlns=\"urn:int:nato:stanag4676:0.14\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" schemaVersion=\"0.14\">\n");
			indentLevel++;
			printout.write(indent() + "<stanagVersion>" + stanagVersion + "</stanagVersion>\n");

			printout.write(indent() + "<messageSecurity>");
			Encode(msg.getSecurity());
			printout.write("</messageSecurity>\n");

			printout.write(indent() + "<msgCreatedTime>" + EncodeTime(msg.getMessageTime()) + "</msgCreatedTime>\n");

			printout.write(indent() + "<senderId>");
			Encode(msg.getSenderID());
			printout.write("</senderId>\n");
			for (final TrackEvent trackevent : msg.getTracks()) {
				printout.write(indent() + "<tracks>");
				Encode(trackevent);
				printout.write("</tracks>\n");
			}
			indentLevel--;
			printout.write("</TrackMessage>\n");
			printout.flush();
		}
	}

	private void Encode(
			Security sec ) {
		if (sec == null) {
			sec = defaultSecurity;
		}
		printout.write("\n");
		indentLevel++;
		printout.write(indent() + "<securityClassification>" + sec.getClassification() + "</securityClassification>\n");
		printout.write(indent() + "<securityPolicyName>" + sec.getPolicyName() + "</securityPolicyName>\n");
		if (sec.getControlSystem() != null) {
			printout.write(indent() + "<securityControlSystem>" + sec.getControlSystem() + "</securityControlSystem>\n");
		}
		if (sec.getDissemination() != null) {
			printout.write(indent() + "<securityDissemination>" + sec.getDissemination() + "</securityDissemination>\n");
		}
		if (sec.getReleasability() != null) {
			printout.write(indent() + "<securityReleasability>" + sec.getReleasability() + "</securityReleasability>\n");
		}
		indentLevel--;
		printout.write(indent());
	}

	private void Encode(
			final IDdata id ) {
		printout.write("\n");
		indentLevel++;
		printout.write(indent() + "<stationID>" + id.getStationId() + "</stationID>\n");
		printout.write(indent() + "<nationality>" + id.getNationality() + "</nationality>\n");
		indentLevel--;
		printout.write(indent());
	}

	private void Encode(
			final TrackEvent event ) {
		printout.write("\n");
		indentLevel++;
		printout.write(indent() + "<trackUUID>" + event.getUuid() + "</trackUUID>\n");
		printout.write(indent() + "<trackNumber>" + event.getTrackNumber() + "</trackNumber>\n");
		if (event.getStatus() != null) {
			printout.write(indent() + "<trackStatus>" + event.getStatus() + "</trackStatus>\n");
		}
		printout.write(indent() + "<trackSecurity>");
		Encode(event.getSecurity());
		printout.write("</trackSecurity>\n");
		if (event.getComment() != null) {
			printout.write(indent() + "<trackComment>" + event.getComment() + "</trackComment>\n");
		}
		if (event.getMissionId() != null) {
			printout.write(indent() + "<missionID>" + event.getMissionId() + "</missionID>\n");
		}
		if (event.getExerciseIndicator() != null) {
			printout.write(indent() + "<exerciseIndicator>" + event.getExerciseIndicator() + "</exerciseIndicator>\n");
		}
		else {
			printout.write(indent() + "<exerciseIndicator>" + defaultExerciseIndicator + "</exerciseIndicator>\n");
		}
		if (event.getSimulationIndicator() != null) {
			printout.write(indent() + "<simulationIndicator>" + event.getSimulationIndicator() + "</simulationIndicator>\n");
		}
		else {
			printout.write(indent() + "<simulationIndicator>" + defaultSimulationIndicator + "</simulationIndicator>\n");
		}
		for (final TrackPoint point : event.getPoints().values()) {
			printout.write(indent() + "<items xsi:type=\"TrackPoint\">");
			Encode(point);
			printout.write("</items>\n");
		}
		for (final TrackIdentity identity : event.getIdentities()) {
			printout.write(indent() + "<items xsi:type=\"TrackIdentityInformation\">");
			Encode(identity);
			printout.write("</items>\n");
		}
		for (final TrackClassification classification : event.getClassifications()) {
			printout.write(indent() + "<items xsi:type=\"TrackClassificationInformation\">");
			Encode(classification);
			printout.write("</items>\n");
		}
		for (final TrackManagement management : event.getManagements()) {
			printout.write(indent() + "<items xsi:type=\"TrackManagementInformation\">");
			Encode(management);
			printout.write("</items>\n");
		}

		for (final MotionImagery image : event.getMotionImages()) {
			printout.write(indent() + "<items xsi:type=\"MotionImageryInformation\">");
			Encode(image);
			printout.write("</items>\n");
		}
		// TODO: ESMInformation
		// TODO: TrackLineageInformation
		indentLevel--;
		printout.write(indent());
	}

	private void Encode(
			final TrackPoint point ) {
		printout.write("\n");
		indentLevel++;
		printout.write(indent() + "<trackItemUUID>" + point.getUuid() + "</trackItemUUID>\n");

		printout.write(indent() + "<trackItemSecurity>");
		Encode(point.getSecurity());
		printout.write("</trackItemSecurity>\n");

		printout.write(indent() + "<trackItemTime>");
		printout.write(EncodeTime(point.getEventTime()));
		printout.write("</trackItemTime>\n");

		if (point.getTrackItemSource() != null) {
			printout.write(indent() + "<trackItemSource>" + point.getTrackItemSource() + "</trackItemSource>\n");
		}
		if (point.getTrackItemComment() != null) {
			printout.write(indent() + "<trackItemComment>" + point.getTrackItemComment() + "</trackItemComment>\n");
		}

		printout.write(indent() + "<trackPointPosition>");
		Encode(point.getLocation());
		printout.write("</trackPointPosition>\n");

		if (point.getSpeed() != null) {
			printout.write(indent() + "<trackPointSpeed>" + point.getSpeed().intValue() + "</trackPointSpeed>\n");
		}
		if (point.getCourse() != null) {
			printout.write(indent() + "<trackPointCourse>" + point.getCourse() + "</trackPointCourse>\n");
		}
		if (point.getTrackPointType() != null) {
			printout.write(indent() + "<trackPointType>" + point.getTrackPointType() + "</trackPointType>\n");
		}
		if (point.getTrackPointSource() != null) {
			printout.write(indent() + "<trackPointSource>" + point.getTrackPointSource() + "</trackPointSource>\n");
		}
		// TODO: need objectMask here
		if (point.getDetail() != null) {
			printout.write(indent() + "<TrackPointDetail>");
			Encode(point.getDetail());
			printout.write("</TrackPointDetail>\n");
		}
		indentLevel--;
		printout.write(indent());
	}

	private void Encode(
			final TrackIdentity identity ) {
		// TODO: Encode TrackIdentity
	}

	private void Encode(
			final TrackClassification classification ) {
		// TODO: Encode TrackClassification
	}

	private void Encode(
			final TrackManagement management ) {
		// TODO: Encode TrackManagement
	}

	private void Encode(
			final MotionImagery image ) {
		printout.write("\n");
		indentLevel++;
		if (image.getBand() != null) {
			printout.write(indent() + "<band>" + image.getBand().toString() + "</band>\n");
		}
		if (image.getImageReference() != null) {
			printout.write(indent() + "<imageReference>" + image.getImageReference() + "</imageReference>\n");
		}
		if (image.getImageChip() != null) {
			printout.write(indent() + "<imageChip>");
			EncodeImage(image.getImageChip());
			printout.write("</imageChip>\n");
		}
		indentLevel--;
		printout.write(indent());
	}

	private void EncodeImage(
			final String base64imageChip ) {
		printout.write("\n");
		indentLevel++;
		printout.write(indent() + "<![CDATA[" + base64imageChip + "]]>\n");
		indentLevel--;
		printout.write(indent());
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
		printout.write("\n");
		indentLevel++;
		printout.write(indent() + "<latitude>" + pos.latitude + "</latitude>\n");
		printout.write(indent() + "<longitude>" + pos.longitude + "</longitude>\n");
		if (pos.elevation != null) {
			printout.write(indent() + "<elevation>" + pos.elevation + "</elevation>\n");
		}
		indentLevel--;
		printout.write(indent());
	}

	private void Encode(
			final TrackPointDetail detail ) {
		printout.write("\n");
		indentLevel++;
		printout.write(indent() + "<pointDetailPosition xsi:type=\"GeodeticPosition\">");
		Encode(detail.getLocation());
		printout.write("</pointDetailPosition>\n");

		if ((detail.getVelocityX() != null) || (detail.getVelocityY() != null) || (detail.getVelocityZ() != null)) {
			printout.write(indent() + "<pointDetailVelocity xsi:type=\"LocalCartesianVelocity\">\n");
			indentLevel++;
			if (detail.getVelocityX() != null) {
				printout.write(indent() + "<velx>" + detail.getVelocityX() + "</velx>\n");
			}
			else {
				printout.write(indent() + "<velx>0</velx>\n");
			}
			if (detail.getVelocityY() != null) {
				printout.write(indent() + "<vely>" + detail.getVelocityY() + "</vely>\n");
			}
			else {
				printout.write(indent() + "<vely>0</vely>\n");
			}
			if (detail.getVelocityZ() != null) {
				printout.write(indent() + "<velz>" + detail.getVelocityZ() + "</velz>\n");
			}
			else {
				printout.write(indent() + "<velz>0</velz>\n");
			}
			indentLevel--;
			printout.write(indent() + "</pointDetailVelocity>\n");
		}
		if ((detail.getAccelerationX() != null) || (detail.getAccelerationY() != null) || (detail.getAccelerationZ() != null)) {
			printout.write(indent() + "<pointDetailAcceleration xsi:type=\"LocalCartesianAcceleration\">\n");
			indentLevel++;
			if (detail.getAccelerationX() != null) {
				printout.write(indent() + "<accx>" + detail.getAccelerationX() + "</accx>\n");
			}
			else {
				printout.write(indent() + "<accx>0</accx>\n");
			}
			if (detail.getAccelerationY() != null) {
				printout.write(indent() + "<accy>" + detail.getAccelerationY() + "</accy>\n");
			}
			else {
				printout.write(indent() + "<accy>0</accy>\n");
			}
			if (detail.getAccelerationZ() != null) {
				printout.write(indent() + "<accz>" + detail.getAccelerationZ() + "</accz>\n");
			}
			else {
				printout.write(indent() + "<accz>0</accz>\n");
			}
			indentLevel--;
			printout.write(indent() + "</pointDetailAcceleration>\n");
		}
		printout.write(indent() + "<pointDetailCovarianceMatrix xsi:type=\"CovarianceMatrixPositionVelocity\">");
		Encode(detail.getCovarianceMatrix());
		printout.write("</pointDetailCovarianceMatrix>\n");
		indentLevel--;
		printout.write(indent());
	}

	private void Encode(
			final CovarianceMatrix cov ) {
		printout.write("\n");
		indentLevel++;
		printout.write(indent() + "<covPosxPosx>" + cov.getCovPosXPosX() + "</covPosxPosx>\n");
		printout.write(indent() + "<covPosyPosy>" + cov.getCovPosYPosY() + "</covPosyPosy>\n");
		if (cov.getCovPosZPosZ() != null) {
			printout.write(indent() + "<covPoszPosz>" + cov.getCovPosZPosZ() + "</covPoszPosz>\n");
		}
		if (cov.getCovPosXPosY() != null) {
			printout.write(indent() + "<covPosxPosy>" + cov.getCovPosXPosY() + "</covPosxPosy>\n");
		}
		if (cov.getCovPosXPosZ() != null) {
			printout.write(indent() + "<covPosxPosz>" + cov.getCovPosXPosZ() + "</covPosxPosz>\n");
		}
		if (cov.getCovPosYPosZ() != null) {
			printout.write(indent() + "<covPosyPosz>" + cov.getCovPosYPosZ() + "</covPosyPosz>\n");
		}
		// these are also optional
		if (cov.getCovVelXVelX() != null) {
			printout.write(indent() + "<covVelxVelx>" + cov.getCovVelXVelX() + "</covVelxVelx>\n");
		}
		if (cov.getCovVelYVelY() != null) {
			printout.write(indent() + "<covVelyVely>" + cov.getCovVelYVelY() + "</covVelyVely>\n");
		}
		//
		if (cov.getCovVelZVelZ() != null) {
			printout.write(indent() + "<covVelzVelz>" + cov.getCovVelZVelZ() + "</covVelzVelz>\n");
		}
		if (cov.getCovPosXVelX() != null) {
			printout.write(indent() + "<covPosxVelx>" + cov.getCovPosXVelX() + "</covPosxVelx>\n");
		}
		if (cov.getCovPosXVelY() != null) {
			printout.write(indent() + "<covPosxVely>" + cov.getCovPosXVelY() + "</covPosxVely>\n");
		}
		if (cov.getCovPosXVelZ() != null) {
			printout.write(indent() + "<covPosxVelz>" + cov.getCovPosXVelZ() + "</covPosxVelz>\n");
		}
		if (cov.getCovPosYVelX() != null) {
			printout.write(indent() + "<covPosyVelx>" + cov.getCovPosYVelX() + "</covPosyVelx>\n");
		}
		if (cov.getCovPosYVelY() != null) {
			printout.write(indent() + "<covPosyVely>" + cov.getCovPosYVelY() + "</covPosyVely>\n");
		}
		if (cov.getCovPosYVelZ() != null) {
			printout.write(indent() + "<covPosyVelz>" + cov.getCovPosYVelZ() + "</covPosyVelz>\n");
		}
		if (cov.getCovPosZVelX() != null) {
			printout.write(indent() + "<covPoszVelx>" + cov.getCovPosZVelX() + "</covPoszVelx>\n");
		}
		if (cov.getCovPosZVelY() != null) {
			printout.write(indent() + "<covPoszVely>" + cov.getCovPosZVelY() + "</covPoszVely>\n");
		}
		if (cov.getCovPosZVelZ() != null) {
			printout.write(indent() + "<covPoszVelz>" + cov.getCovPosZVelZ() + "</covPoszVelz>\n");
		}
		if (cov.getCovVelXVelY() != null) {
			printout.write(indent() + "<covVelxVely>" + cov.getCovVelXVelY() + "</covVelxVely>\n");
		}
		if (cov.getCovVelXVelZ() != null) {
			printout.write(indent() + "<covVelxVelz>" + cov.getCovVelXVelZ() + "</covVelxVelz>\n");
		}
		if (cov.getCovVelYVelZ() != null) {
			printout.write(indent() + "<covVelyVelz>" + cov.getCovVelYVelZ() + "</covVelyVelz>\n");
		}
		indentLevel--;
		printout.write(indent());
	}

}
