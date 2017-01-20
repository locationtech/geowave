package mil.nga.giat.geowave.format.stanag4676.parser.model;

public class MissionFrame
{
	private Area coverageArea;
	private long frameTime;
	private String missionId;
	private Integer frameNumber;

	/**
	 * @return the missionId
	 */
	public String getMissionId() {
		return missionId;
	}

	/**
	 * @param missionId
	 *            the missionId to set
	 */
	public void setMissionId(
			String missionId ) {
		this.missionId = missionId;
	}

	/**
	 * @return the frameNumber
	 */
	public Integer getFrameNumber() {
		return frameNumber;
	}

	/**
	 * @param frameNumber
	 *            the frameNumber to set
	 */
	public void setFrameNumber(
			Integer frameNumber ) {
		this.frameNumber = frameNumber;
	}

	/**
	 * @return the frameTime
	 */
	public long getFrameTime() {
		return frameTime;
	}

	/**
	 * @param frameTime
	 *            the frameTime to set
	 */
	public void setFrameTime(
			long frameTime ) {
		this.frameTime = frameTime;
	}

	/**
	 * @return the coverageArea
	 */
	public Area getCoverageArea() {
		return coverageArea;
	}

	/**
	 * @param coverageArea
	 *            the coverageArea to set
	 */
	public void setCoverageArea(
			Area coverageArea ) {
		this.coverageArea = coverageArea;
	}
}