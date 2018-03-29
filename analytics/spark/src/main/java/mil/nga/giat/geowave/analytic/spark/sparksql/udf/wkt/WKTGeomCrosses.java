package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomCrosses;

public class WKTGeomCrosses extends
		WKTGeomFunction
{
	public WKTGeomCrosses() {
		super(
				new GeomCrosses());
	}
}
