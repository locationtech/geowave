package mil.nga.giat.geowave.analytic.mapreduce.nn;

/**
 * Retain distance information.
 * 
 */
public class DistanceProfile<CONTEXT_TYPE>
{
	private double distance;
	private CONTEXT_TYPE context;

	public DistanceProfile() {

	}

	public DistanceProfile(
			double distance,
			CONTEXT_TYPE context ) {
		super();
		this.distance = distance;
		this.context = context;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(
			double distance ) {
		this.distance = distance;
	}

	/**
	 * 
	 * distance function specific information
	 */
	public CONTEXT_TYPE getContext() {
		return context;
	}

	public void setContext(
			CONTEXT_TYPE context ) {
		this.context = context;
	}

	@Override
	public String toString() {
		return "DistanceProfile [distance=" + distance + ", context=" + context + "]";
	}

}
