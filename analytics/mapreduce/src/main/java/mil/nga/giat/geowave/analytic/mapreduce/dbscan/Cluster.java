package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.Set;

import mil.nga.giat.geowave.analytic.nn.NeighborList;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import com.vividsolutions.jts.geom.Geometry;

public interface Cluster extends
		NeighborList<ClusterItem>
{
	public void merge(
			Cluster cluster );

	public ByteArrayId getId();

	/*
	 * Return the cluster to which this cluster is linked
	 */
	public Set<ByteArrayId> getLinkedClusters();

	public int currentLinkSetSize();

	public void invalidate();

	public void finish();

	public boolean isCompressed();

	public Geometry getGeometry();

}
