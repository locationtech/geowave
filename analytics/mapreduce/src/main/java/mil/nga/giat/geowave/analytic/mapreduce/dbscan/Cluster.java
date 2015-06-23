package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.Iterator;

import mil.nga.giat.geowave.analytic.mapreduce.nn.NeighborList;
import mil.nga.giat.geowave.core.index.ByteArrayId;

public interface Cluster<NNTYPE> extends
		NeighborList<NNTYPE>
{
	public void merge(
			Cluster<NNTYPE> cluster );

	public ByteArrayId getId();

	/*
	 * Return the cluster to which this cluster is linked
	 */
	public Iterator<Cluster<NNTYPE>> getLinkedClusters();

	public Iterator<ByteArrayId> clusteredIds();
}
