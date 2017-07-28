package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

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

	private static GeomEquals geomEqualsInstance = new GeomEquals();
	private static GeomWithin geomWithinInstance = new GeomWithin();
	private static GeomContains geomContainsInstance = new GeomContains();
	private static GeomIntersects geomIntersectsInstance = new GeomIntersects();
	private static GeomCrosses geomCrossesInstance = new GeomCrosses();
	private static GeomTouches geomTouchesInstance = new GeomTouches();
	private static GeomCovers geomCoversInstance = new GeomCovers();
	private static GeomDisjoint geomDisjointInstance = new GeomDisjoint();
	private static GeomOverlaps geomOverlapsInstance = new GeomOverlaps();

	public static void registerGeometryFunctions(
			SparkSession spark ) {

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
