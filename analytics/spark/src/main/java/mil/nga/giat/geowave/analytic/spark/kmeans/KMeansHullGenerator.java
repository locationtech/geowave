package mil.nga.giat.geowave.analytic.spark.kmeans;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;

public class KMeansHullGenerator
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KMeansHullGenerator.class);

	public static JavaPairRDD<Integer, Iterable<Vector>> groupByIndex(
			final JavaRDD<Vector> inputPoints,
			final KMeansModel clusterModel ) {
		// Group the input points by their kmeans centroid index
		return inputPoints.groupBy(
				point -> {
					return clusterModel.predict(
							point);
				});
	}

	public static JavaPairRDD<Integer, Geometry> generateHullsRDD(
			final JavaPairRDD<Integer, Iterable<Vector>> groupedPoints ) {
		// Create the convex hull for each kmeans centroid
		final JavaPairRDD<Integer, Geometry> hullRDD = groupedPoints.mapValues(
				point -> {
					final Iterable<Coordinate> coordIt = Iterables.transform(
							point,
							new com.google.common.base.Function<Vector, Coordinate>() {
								@Override
								public Coordinate apply(
										final Vector input ) {
									if (input != null) {
										return new Coordinate(
												input.apply(
														0),
												input.apply(
														1));
									}

									return new Coordinate();
								}
							});

					final Coordinate[] coordArray = Iterables.toArray(
							coordIt,
							Coordinate.class);

					return new ConvexHull(
							coordArray,
							GeometryUtils.GEOMETRY_FACTORY).getConvexHull();
				});

		return hullRDD;
	}
}
