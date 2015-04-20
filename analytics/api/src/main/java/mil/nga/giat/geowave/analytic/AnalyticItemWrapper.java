package mil.nga.giat.geowave.analytic;

import com.vividsolutions.jts.geom.Geometry;

/**
 * 
 * Wrap an object used to by analytical processes. This class provides generic
 * wrapper to specific functions associated with analytic processes such as
 * managing centroids.
 * 
 * 
 * @param <T>
 */
public interface AnalyticItemWrapper<T>
{
	public String getID();

	public T getWrappedItem();

	public long getAssociationCount();

	public void resetAssociatonCount();

	public void incrementAssociationCount(
			long increment );

	public int getIterationID();

	public String getName();

	public String[] getExtraDimensions();

	public double[] getDimensionValues();

	public Geometry getGeometry();

	public double getCost();

	public void setCost(
			double cost );

	public String getGroupID();

	public void setGroupID(
			String groupID );

	public void setZoomLevel(
			int level );

	public int getZoomLevel();

	public void setBatchID(
			String batchID );

	public String getBatchID();

}
