package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomContains;

public class WKTGeomContains extends
		WKTGeomFunction
{
	public WKTGeomContains() {
		super(
				new GeomContains());
	}
}
