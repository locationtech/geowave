package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomOverlaps;

public class WKTGeomOverlaps extends
		WKTGeomFunction
{
	public WKTGeomOverlaps() {
		super(
				new GeomOverlaps());
	}
}
