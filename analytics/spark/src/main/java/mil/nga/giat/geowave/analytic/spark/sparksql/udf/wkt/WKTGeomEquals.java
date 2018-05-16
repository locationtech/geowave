package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomEquals;

public class WKTGeomEquals extends
		WKTGeomFunction
{
	public WKTGeomEquals() {
		super(
				new GeomEquals());
	}
}
