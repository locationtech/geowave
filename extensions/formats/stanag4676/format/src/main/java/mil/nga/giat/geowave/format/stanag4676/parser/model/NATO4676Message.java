package mil.nga.giat.geowave.format.stanag4676.parser.model;

public class NATO4676Message
{
	protected String formatVersion;
	protected long messageTime;
	protected Security security;
	protected IDdata senderID;
	protected Long runId;

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

	public Long getRunId() {
		return runId;
	}

	public void setRunId(
			Long id ) {
		this.runId = id;
	}
}