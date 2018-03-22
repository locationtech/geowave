package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import com.vividsolutions.jts.geom.Geometry;

public class GeomOverlaps extends
		GeomFunction
{
	@Override
	public boolean apply(
			Geometry geom1,
			Geometry geom2 ) {
		return geom1.overlaps(geom2);
	}
}
