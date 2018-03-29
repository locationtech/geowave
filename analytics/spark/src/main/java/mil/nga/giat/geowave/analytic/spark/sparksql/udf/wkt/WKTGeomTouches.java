package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomTouches;

public class WKTGeomTouches extends
		WKTGeomFunction
{
	public WKTGeomTouches() {
		super(
				new GeomTouches());
	}
}
