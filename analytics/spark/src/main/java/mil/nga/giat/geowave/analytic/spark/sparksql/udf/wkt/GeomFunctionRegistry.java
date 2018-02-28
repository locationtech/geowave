package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import java.io.Serializable;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeomFunctionRegistry implements
		Serializable
{
	private static final long serialVersionUID = -1729498500215830962L;
	private final static Logger LOGGER = LoggerFactory.getLogger(GeomFunctionRegistry.class);

	private static WKTGeomEquals geomEqualsInstance = new WKTGeomEquals();
	private static WKTGeomWithin geomWithinInstance = new WKTGeomWithin();
	private static WKTGeomContains geomContainsInstance = new WKTGeomContains();
	private static WKTGeomIntersects geomIntersectsInstance = new WKTGeomIntersects();
	private static WKTGeomCrosses geomCrossesInstance = new WKTGeomCrosses();
	private static WKTGeomTouches geomTouchesInstance = new WKTGeomTouches();
	private static WKTGeomCovers geomCoversInstance = new WKTGeomCovers();
	private static WKTGeomDisjoint geomDisjointInstance = new WKTGeomDisjoint();
	private static WKTGeomOverlaps geomOverlapsInstance = new WKTGeomOverlaps();
	private static WKTGeomDistance geomDistanceInstance = new WKTGeomDistance();

	public static void registerGeometryFunctions(
			SparkSession spark ) {

		spark.udf().register(
				"geomDistance",
				geomDistanceInstance,
				DataTypes.DoubleType);

		spark.udf().register(
				"geomEquals",
				geomEqualsInstance,
				DataTypes.BooleanType);

		spark.udf().register(
				"geomWithin",
				geomWithinInstance,
				DataTypes.BooleanType);

		spark.udf().register(
				"geomContains",
				geomContainsInstance,
				DataTypes.BooleanType);

		spark.udf().register(
				"geomIntersects",
				geomIntersectsInstance,
				DataTypes.BooleanType);

		spark.udf().register(
				"geomCrosses",
				geomCrossesInstance,
				DataTypes.BooleanType);

		spark.udf().register(
				"geomTouches",
				geomTouchesInstance,
				DataTypes.BooleanType);

		spark.udf().register(
				"geomCovers",
				geomCoversInstance,
				DataTypes.BooleanType);

		spark.udf().register(
				"geomDisjoint",
				geomDisjointInstance,
				DataTypes.BooleanType);

		spark.udf().register(
				"geomOverlaps",
				geomOverlapsInstance,
				DataTypes.BooleanType);
	}
}
