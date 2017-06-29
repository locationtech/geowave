package mil.nga.giat.geowave.analytic.javaspark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;

public class KMeansHullGenerator
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			KMeansHullGenerator.class);

	public static JavaRDD<Geometry> generateHullsRDD(
			JavaRDD<Vector> inputPoints,
			KMeansModel clusterModel ) {

		// Group the input points by their kmeans centroid index
		JavaPairRDD<Integer, Iterable<Vector>> pointGroupRDD = inputPoints.groupBy(
				new Function<Vector, Integer>() {
					@Override
					public Integer call(
							Vector point )
							throws Exception {
						return clusterModel.predict(
								point);
					}
				});

		LOGGER.warn(
				"GroupBy RDD has " + pointGroupRDD.count() + " rows");

		// Create the convex hull for each kmeans centroid
		JavaPairRDD<Integer, Iterable<Vector>> hullRDD = pointGroupRDD.reduceByKey(
				new Function2<Iterable<Vector>, Iterable<Vector>, Iterable<Vector>>() {
					@Override
					public Iterable<Vector> call(
							Iterable<Vector> accum,
							Iterable<Vector> nextVert )
							throws Exception {
						// Create the geometry from the input list
						Geometry hull = GeometryUtils.GEOMETRY_FACTORY.buildGeometry(
								Collections.EMPTY_LIST);

						while (accum.iterator().hasNext()) {
							Vector point = accum.iterator().next();
							hull = hull.union(
									GeometryUtils.GEOMETRY_FACTORY.createPoint(
											new Coordinate(
													point.apply(
															0),
													point.apply(
															1))));
						}

						// Add the next point to the hull
						if (nextVert.iterator().hasNext()) {
							Vector point = nextVert.iterator().next();

							hull = hull.union(
									GeometryUtils.GEOMETRY_FACTORY.createPoint(
											new Coordinate(
													point.apply(
															0),
													point.apply(
															1))));

							hull = hull.convexHull();
						}

						// Unload the convex hull into the output list
						ArrayList<Vector> hullVerts = new ArrayList<Vector>();

						for (Coordinate coord : hull.getCoordinates()) {
							double[] values = new double[2];
							values[0] = coord.x;
							values[1] = coord.y;
							Vector vert = Vectors.dense(
									values);

							hullVerts.add(
									vert);
						}
						
						LOGGER.warn("Hull verts size = " + hullVerts.size());

						return hullVerts;
					}
				});

		LOGGER.warn(
				"Hull verts RDD has " + hullRDD.count() + " rows");
		
		// Map the hull vectors to geometry
		JavaRDD<Geometry> geomRDD = hullRDD.values().map(
				new Function<Iterable<Vector>, Geometry>() {

					@Override
					public Geometry call(
							Iterable<Vector> verts )
							throws Exception {
						// Create the geometry from the input list
						Geometry hull = GeometryUtils.GEOMETRY_FACTORY.buildGeometry(
								Collections.EMPTY_LIST);

						while (verts.iterator().hasNext()) {
							Vector point = verts.iterator().next();
							hull = hull.union(
									GeometryUtils.GEOMETRY_FACTORY.createPoint(
											new Coordinate(
													point.apply(
															0),
													point.apply(
															1))));
						}

						return hull;
					}
				});

		LOGGER.warn(
				"Hull geom RDD has " + geomRDD.count() + " rows");

		return geomRDD;
	}

	public static Geometry[] generateHullsLocal(
			JavaRDD<Vector> inputCentroids,
			KMeansModel clusterModel ) {
		final Geometry[] hulls = new Geometry[clusterModel.clusterCenters().length];

		List<Vector> inputList = inputCentroids.collect();

		// Run each input through the model to get its centroid and create the
		// hull
		for (Vector point : inputList) {
			int centroidIndex = clusterModel.predict(
					point);

			if (hulls[centroidIndex] == null) {
				hulls[centroidIndex] = GeometryUtils.GEOMETRY_FACTORY.buildGeometry(
						Collections.EMPTY_LIST);
			}

			Coordinate coord = new Coordinate(
					point.apply(
							0),
					point.apply(
							1));

			Geometry union = hulls[centroidIndex].union(
					GeometryUtils.GEOMETRY_FACTORY.createPoint(
							coord));

			hulls[centroidIndex] = union.convexHull();
		}

		return hulls;
	}
}
