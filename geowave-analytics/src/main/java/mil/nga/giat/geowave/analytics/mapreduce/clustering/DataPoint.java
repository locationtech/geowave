package mil.nga.giat.geowave.analytics.mapreduce.clustering;

public class DataPoint
{
	public Double x;
	public Double y;
	public Integer assignedCentroidId = -1;
	public Integer id;
	public boolean isCentroid = false;

	public DataPoint() {
		// stub
	}

	public DataPoint(
			final int id,
			final double x,
			final double y,
			final int centroidId,
			final boolean isCentroid ) {
		this.id = id;
		this.x = x;
		this.y = y;
		this.isCentroid = isCentroid;
		assignedCentroidId = centroidId;
	}

	public double calculateDistance(
			final DataPoint dp ) {
		// Euclidean distance
		return Math.sqrt(Math.pow(
				Math.abs(x - dp.x),
				2) + Math.pow(
				Math.abs(y - dp.y),
				2));
	}

	@Override
	public String toString() {
		return "id: " + id + ", coordinate: (" + x + ", " + y + ")";
	}
}
