package mil.nga.giat.geowave.analytics.mapreduce.clustering;

public class DataPoint {
	public Double x;
	public Double y;
	public Integer assignedCentroidId = -1;
	public Integer id;
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
	
	public String toString()
	{
		return "id: " + id + ", coordinate: (" + x + ", " + y + ")";
	}
}
