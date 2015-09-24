package mil.nga.giat.geowave.format.stanag4676.parser.model;

import java.util.UUID;

public class TrackItem
{
	private UUID uuid;
	private Security security;
	private long time;
	private String source;
	private String comment;

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(
			UUID uuid ) {
		this.uuid = uuid;
	}

	public Security getSecurity() {
		return security;
	}

	public void setSecurity(
			Security security ) {
		this.security = security;
	}

	public long getTime() {
		return time;
	}

	public void setTime(
			long time ) {
		this.time = time;
	}

	public String getSource() {
		return source;
	}

	public void setSource(
			String source ) {
		this.source = source;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(
			String comment ) {
		this.comment = comment;
	}

}
