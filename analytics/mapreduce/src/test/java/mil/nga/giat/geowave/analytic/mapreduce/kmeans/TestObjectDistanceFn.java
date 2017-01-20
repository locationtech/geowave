package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import mil.nga.giat.geowave.analytic.distance.CoordinateEuclideanDistanceFn;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class TestObjectDistanceFn implements
		DistanceFn<TestObject>
{
	private DistanceFn<Coordinate> coordinateDistanceFunction = new CoordinateEuclideanDistanceFn();

	private Geometry getGeometry(
			TestObject x ) {
		return x.geo;
	}

	@Override
	public double measure(
			TestObject x,
			TestObject y ) {

		return coordinateDistanceFunction.measure(
				getGeometry(
						x).getCentroid().getCoordinate(),
				getGeometry(
						y).getCentroid().getCoordinate());
	}

}
