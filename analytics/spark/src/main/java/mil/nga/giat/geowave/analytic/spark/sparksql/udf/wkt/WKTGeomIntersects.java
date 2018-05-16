package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomIntersects;

public class WKTGeomIntersects extends
		WKTGeomFunction
{
	public WKTGeomIntersects() {
		super(
				new GeomIntersects());
	}
}
