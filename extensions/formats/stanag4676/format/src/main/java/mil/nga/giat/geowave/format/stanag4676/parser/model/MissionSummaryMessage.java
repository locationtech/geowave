package mil.nga.giat.geowave.format.stanag4676.parser.model;

public class MissionSummaryMessage extends
		NATO4676Message
{
	private MissionSummary missionSummary = new MissionSummary();

	/**
	 * @return the missionSummary
	 */
	public MissionSummary getMissionSummary() {
		return missionSummary;
	}

	/**
	 * @param missionSummary
	 *            the missionSummary to set
	 */
	public void setMissionSummary(
			MissionSummary missionSummary ) {
		this.missionSummary = missionSummary;
	}
}