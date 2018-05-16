package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomDisjoint;

public class WKTGeomDisjoint extends
		WKTGeomFunction
{
	public WKTGeomDisjoint() {
		super(
				new GeomDisjoint());
	}
}
