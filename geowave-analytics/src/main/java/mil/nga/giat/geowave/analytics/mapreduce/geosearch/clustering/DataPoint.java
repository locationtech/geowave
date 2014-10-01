package mil.nga.giat.geowave.analytics.mapreduce.geosearch.clustering;

public class DataPoint {
	public double x;
	public double y;
	public int assignedCentroidId = -1;
	public int id;
	public boolean isCentroid = false;

	public DataPoint()
	{
		// stub
	}
	
	public DataPoint(int id, double x, double y, int centroidId, boolean isCentroid)
	{
		this.id = id;
		this.x = x;
		this.y = y;
		this.isCentroid = isCentroid;
		this.assignedCentroidId = centroidId;
	}
	
	public double calculateDistance(DataPoint dp)
	{		
		// Euclidean distance 
		return Math.sqrt(Math.pow(Math.abs(x - dp.x), 2) + Math.pow(Math.abs(y - dp.y), 2));
	}
}
