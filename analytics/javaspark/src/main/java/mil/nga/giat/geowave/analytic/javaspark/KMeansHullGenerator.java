package mil.nga.giat.geowave.analytic.javaspark;

import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;

public class KMeansHullGenerator
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KMeansHullGenerator.class);

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
				hulls[centroidIndex] = GeometryUtils.GEOMETRY_FACTORY.buildGeometry(Collections.EMPTY_LIST);
			}

			Coordinate coord = new Coordinate(
					point.apply(0),
					point.apply(1));

			Geometry union = hulls[centroidIndex].union(GeometryUtils.GEOMETRY_FACTORY.createPoint(coord));

			hulls[centroidIndex] = union.convexHull();
		}

		return hulls;
	}
}
