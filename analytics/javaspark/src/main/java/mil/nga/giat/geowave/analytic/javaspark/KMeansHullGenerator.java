package mil.nga.giat.geowave.analytic.javaspark;

import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class KMeansHullGenerator
{
	private static GeometryFactory geomFactory = new GeometryFactory();

	public static Geometry[] generateHulls(
			JavaRDD<Vector> inputCentroids,
			KMeansModel clusterModel ) {
		final Geometry[] hulls = new Geometry[clusterModel.clusterCenters().length];

		List<Vector> inputList = inputCentroids.collect();

		// Run each input through the model to get its centroid and create the
		// hull
		for (Vector point : inputList) {
			int centroidIndex = clusterModel.predict(point);

			if (hulls[centroidIndex] == null) {
				hulls[centroidIndex] = geomFactory.buildGeometry(Collections.EMPTY_LIST);
			}

			Coordinate coord = new Coordinate(
					point.apply(0),
					point.apply(1));

			Geometry union = hulls[centroidIndex].union(geomFactory.createPoint(coord));

			hulls[centroidIndex] = union.convexHull();
		}

		return hulls;
	}
}
