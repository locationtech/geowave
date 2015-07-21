package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.analytic.GeometryHullTool;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.nn.DistanceProfile;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NeighborList;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NeighborListFactory;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.TopologyException;

/**
 * 
 * A cluster represented by a hull.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 * 
 * TODO: connectGeometryTool.connect(
 */
public class ClusterUnionList extends
		DBScanClusterList implements
		CompressingCluster<ClusterItem, Geometry>
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(ClusterUnionList.class);

	private final GeometryHullTool connectGeometryTool = new GeometryHullTool();

	// internal state
	private Geometry clusterGeo;

	public ClusterUnionList(
			final DistanceFn<Coordinate> distanceFnForCoordinate,
			final ByteArrayId centerId,
			final ClusterItem center,
			final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
		super(
				centerId,
				index);

		clusterGeo = center.getGeometry();

		this.connectGeometryTool.setDistanceFnForCoordinate(distanceFnForCoordinate);

		putCount(
				centerId,
				center.getCount(),
				true);

	}

	protected Long addAndFetchCount(
			final ByteArrayId id,
			final ClusterItem newInstance ) {
		union(newInstance.getGeometry());
		return (Long) newInstance.getCount();
	}

	private void union(
			Geometry otherGeo ) {

		try {
			clusterGeo = clusterGeo.union(otherGeo);
		}
		catch (TopologyException ex) {

			LOGGER.error(
					"Union failed due to non-simple geometries",
					ex);
			clusterGeo = connectGeometryTool.createHullFromGeometry(
					clusterGeo,
					Arrays.asList(otherGeo.getCoordinates()),
					true);
		}
	}

	@Override
	public void merge(
			Cluster<ClusterItem> cluster ) {
		interpolateAddCount((DBScanClusterList) cluster);
		if (cluster != this) {
			union(((ClusterUnionList) cluster).clusterGeo);
		}
	}

	protected Geometry compress() {

		return clusterGeo;
	}

	public static class ClusterUnionListFactory implements
			NeighborListFactory<ClusterItem>
	{
		private final Map<ByteArrayId, Cluster<ClusterItem>> index;
		private final DistanceFn<Coordinate> distanceFnForCoordinate;

		public ClusterUnionListFactory(
				final DistanceFn<Coordinate> distanceFnForCoordinate,
				final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
			super();
			this.distanceFnForCoordinate = distanceFnForCoordinate;
			this.index = index;
		}

		public NeighborList<ClusterItem> buildNeighborList(
				final ByteArrayId centerId,
				final ClusterItem center ) {
			return new ClusterUnionList(
					distanceFnForCoordinate,
					centerId,
					center,
					index);
		}
	}
}
