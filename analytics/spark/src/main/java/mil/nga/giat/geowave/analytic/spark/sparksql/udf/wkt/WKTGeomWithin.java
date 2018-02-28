package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomWithin;

public class WKTGeomWithin extends
		WKTGeomFunction
{
	public WKTGeomWithin() {
		super(
				new GeomWithin());
	}
}
