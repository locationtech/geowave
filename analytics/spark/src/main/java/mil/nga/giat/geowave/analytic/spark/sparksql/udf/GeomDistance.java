package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import org.apache.spark.sql.api.java.UDF2;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;

public class GeomDistance implements
		UDF2<Geometry, Geometry, Double>
{
	@Override
	public Double call(
			Geometry leftGeom,
			Geometry rightGeom )
			throws Exception {
		return leftGeom.distance(rightGeom);

	}

}