package mil.nga.giat.geowave.analytic.spark.spatial;

import java.io.Serializable;

public class JoinOptions implements
		Serializable
{

	public static enum BuildSide {
		LEFT,
		RIGHT;
	}

	private BuildSide joinBuildSide = BuildSide.LEFT;
	private boolean negativePredicate = false;

	public JoinOptions() {}

	public JoinOptions(
			boolean negativeTest ) {
		this.negativePredicate = negativeTest;
	}

	public boolean isNegativePredicate() {
		return negativePredicate;
	}

	public void setNegativePredicate(
			boolean negativePredicate ) {
		this.negativePredicate = negativePredicate;
	}

	public BuildSide getJoinBuildSide() {
		return joinBuildSide;
	}

	public void setJoinBuildSide(
			BuildSide joinBuildSide ) {
		this.joinBuildSide = joinBuildSide;
	}

}
