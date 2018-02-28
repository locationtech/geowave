package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import org.apache.spark.sql.api.java.UDF2;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;

public class WKTGeomDistance implements
		UDF2<String, String, Double>
{

	private GeomReader reader = new GeomReader();

	@Override
	public Double call(
			String t1,
			String t2 )
			throws Exception {
		Geometry leftGeom = reader.read(t1);
		Geometry rightGeom = reader.read(t2);
		if (leftGeom != null && rightGeom != null) return leftGeom.distance(rightGeom);

		return Double.MAX_VALUE;
	}

}