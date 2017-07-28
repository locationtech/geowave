package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import com.vividsolutions.jts.geom.Geometry;

public class GeomCrosses extends
		GeomFunction
{
	@Override
	public Boolean call(
			String geomStr1,
			String geomStr2 )
			throws Exception {
		Geometry geom1 = parseGeom(geomStr1);
		Geometry geom2 = parseGeom(geomStr2);

		if (geom1 != null && geom2 != null) {
			return geom1.crosses(geom2);
		}

		return false;
	}
}
