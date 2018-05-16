package mil.nga.giat.geowave.analytic.spark.spatial;

import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;

public abstract class JoinStrategy implements
		SpatialJoin
{
	// Final joined pair RDDs
	protected GeoWaveRDD leftJoined = null;
	protected GeoWaveRDD rightJoined = null;

	protected JoinOptions joinOpts = new JoinOptions();

	public GeoWaveRDD getLeftResults() {
		return leftJoined;
	}

	public void setLeftResults(
			GeoWaveRDD leftJoined ) {
		this.leftJoined = leftJoined;
	}

	public GeoWaveRDD getRightResults() {
		return rightJoined;
	}

	public void setRightResults(
			GeoWaveRDD rightJoined ) {
		this.rightJoined = rightJoined;
	}

	public JoinOptions getJoinOptions() {
		return joinOpts;
	}

	public void setJoinOptions(
			JoinOptions joinOpts ) {
		this.joinOpts = joinOpts;
	}

}
