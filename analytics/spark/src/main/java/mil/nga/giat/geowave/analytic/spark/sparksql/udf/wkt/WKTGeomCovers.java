package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomCovers;

public class WKTGeomCovers extends
		WKTGeomFunction
{
	public WKTGeomCovers() {
		super(
				new GeomCovers());
	}
}
