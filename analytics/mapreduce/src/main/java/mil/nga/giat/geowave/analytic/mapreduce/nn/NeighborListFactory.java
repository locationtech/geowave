package mil.nga.giat.geowave.analytic.mapreduce.nn;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public interface NeighborListFactory<NNTYPE>
{
	public NeighborList<NNTYPE> buildNeighborList(
			ByteArrayId cnterId,
			NNTYPE center );
}
