package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import com.vividsolutions.jts.geom.Geometry;

public class GeomWithinDistance extends
		GeomFunction
{

	private double radius;

	public GeomWithinDistance(
			double radius ) {
		this.radius = radius;
	}

	public double getBufferAmount() {
		return radius;
	}

	public double getRadius() {
		return radius;
	}

	@Override
	public boolean apply(
			Geometry geom1,
			Geometry geom2 ) {
		return geom1.distance(geom2) <= radius;
	}
}