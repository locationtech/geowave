package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.analytic.GeometryHullTool;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.dbscan.ClusterItemDistanceFn.ClusterProfileContext;
import mil.nga.giat.geowave.analytic.mapreduce.nn.DistanceProfile;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NeighborList;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NeighborListFactory;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

/**
 * 
 * Maintains a single hull around a set of points.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 * 
 * TODO: connectGeometryTool.connect(
 */
public class SingleItemClusterList extends
		DBScanClusterList implements
		CompressingCluster<ClusterItem, Geometry>
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(SingleItemClusterList.class);

	// internal state
	private Geometry clusterGeo;
	private final boolean initializedAsPoint;
	private final Set<Coordinate> clusterPoints = new HashSet<Coordinate>();

	private final GeometryHullTool connectGeometryTool = new GeometryHullTool();

	public SingleItemClusterList(
			final DistanceFn<Coordinate> distanceFnForCoordinate,
			final ByteArrayId centerId,
			final ClusterItem center,
			final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
		super(
				centerId,
				index);

		this.connectGeometryTool.setDistanceFnForCoordinate(distanceFnForCoordinate);

		final Geometry clusterGeo = center.getGeometry();

		this.clusterGeo = clusterGeo.getCentroid();

		initializedAsPoint = clusterGeo instanceof Point;

		if (initializedAsPoint) {
			clusterPoints.add(clusterGeo.getCoordinate());
		}

		this.add(
				centerId,
				center);
	}

	@Override
	protected Long addAndFetchCount(
			final ByteArrayId id,
			final ClusterItem newInstance ) {
		checkForCompression();
		return ONE;
	}

	protected boolean add(
			final DistanceProfile<?> distanceProfile,
			final ByteArrayId id,
			final ClusterItem newInstance ) {
		ClusterProfileContext context = (ClusterProfileContext) distanceProfile.getContext();
		// If initialized from a point, then any hull created during compression
		// contains that point.
		// Adding that point is not needed. Points from coordinates[0] (center)
		// are only added if they are part of more complex geometry.
		if (!initializedAsPoint) {
			final Coordinate centerCoordinate = context.getItem1() == newInstance ? context.getPoint2() : context.getPoint1();
			if (!clusterPoints.contains(centerCoordinate) && (!this.clusterGeo.covers(clusterGeo.getFactory().createPoint(
					centerCoordinate)))) clusterPoints.add(centerCoordinate);
		}
		final Coordinate newInstanceCoordinate = context.getItem2() == newInstance ? context.getPoint2() : context.getPoint1();
		// optimization to avoid creating a point if a representative one
		// already
		// exists
		if (newInstance.getGeometry() instanceof Point) {
			if (!clusterGeo.covers(newInstance.getGeometry())) clusterPoints.add(newInstanceCoordinate);
		}
		else {
			// need to create point since the provided coordinate is most likely
			// some point on a segment rather than a vertex
			if (!clusterGeo.covers(clusterGeo.getFactory().createPoint(
					newInstanceCoordinate))) clusterPoints.add(newInstanceCoordinate);
		}

		return super.add(
				distanceProfile,
				id,
				newInstance);
	}

	@Override
	public void merge(
			Cluster<ClusterItem> cluster ) {
		if (this == cluster) return;
		super.merge(cluster);
		this.clusterPoints.addAll(((SingleItemClusterList) cluster).clusterPoints);
		checkForCompression();
	}

	private void checkForCompression() {
		if (clusterPoints.size() > 20) {
			clusterGeo = compress();
			clusterPoints.clear();
		}
	}

	@Override
	protected Geometry compress() {
		return connectGeometryTool.createHullFromGeometry(
				clusterGeo,
				clusterPoints,
				true);

	}

	public static class SingleItemClusterListFactory implements
			NeighborListFactory<ClusterItem>
	{
		private final DistanceFn<Coordinate> distanceFnForCoordinate;
		private final Map<ByteArrayId, Cluster<ClusterItem>> index;

		public SingleItemClusterListFactory(
				final DistanceFn<Coordinate> distanceFnForCoordinate,
				final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
			super();
			this.distanceFnForCoordinate = distanceFnForCoordinate;
			this.index = index;
		}

		public NeighborList<ClusterItem> buildNeighborList(
				final ByteArrayId centerId,
				final ClusterItem center ) {
			return new SingleItemClusterList(
					distanceFnForCoordinate,
					centerId,
					center,
					index);
		}
	}
}
