package mil.nga.giat.geowave.format.stanag4676.parser.model;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TrackMessage
{
	private Long id;

	private UUID uuid;
	private String formatVersion;
	private long messageTime;
	private Security security; // store security tags as a single string for now
	private IDdata senderID;
	private List<TrackEvent> tracks;
	private Long trackRunId;
	private String missionId; // used to pass missionid for the FileMultiWriter.
								// This field is NOT persisted to the database

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

	public void setFormatVersion(
			String formatVersion ) {
		this.formatVersion = formatVersion;
	}

	public String getFormatVersion() {
		return formatVersion;
	}

	public long getMessageTime() {
		return messageTime;
	}

	public void setMessageTime(
			long messageTime ) {
		this.messageTime = messageTime;
	}

	public Security getSecurity() {
		return security;
	}

	public void setSecurity(
			Security security ) {
		this.security = security;
	}

	public IDdata getSenderID() {
		return senderID;
	}

	public void setSenderID(
			IDdata senderID ) {
		this.senderID = senderID;
	}

	public List<TrackEvent> getTracks() {
		return tracks;
	}

	public void setTracks(
			List<TrackEvent> tracks ) {
		this.tracks = tracks;
	}

	public void addTrackEvent(
			TrackEvent trkEvnt ) {
		if (tracks == null) {
			tracks = new ArrayList<TrackEvent>();
		}
		tracks.add(trkEvnt);
	}

	public Long getTrackRunId() {
		return trackRunId;
	}

	public void setTrackRunId(
			Long id ) {
		this.trackRunId = id;
	}

	public void setMissionId(
			String missionId ) {
		this.missionId = missionId;
	}

	public String getMissionId() {
		return this.missionId;
	}
}
