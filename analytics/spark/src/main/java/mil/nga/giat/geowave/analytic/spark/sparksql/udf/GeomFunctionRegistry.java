package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import java.io.Serializable;

import mil.nga.giat.geowave.analytic.spark.sparksql.GeoWaveSpatialEncoders;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.UDFRegistrySPI;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.UDFRegistrySPI.UDFNameAndConstructor;

public class GeomFunctionRegistry implements
		Serializable
{
	private static final long serialVersionUID = -1729498500215830962L;
	private final static Logger LOGGER = LoggerFactory.getLogger(GeomFunctionRegistry.class);

	private static GeomDistance geomDistanceInstance = new GeomDistance();
	private static GeomFromWKT geomWKTInstance = new GeomFromWKT();

	public static void registerGeometryFunctions(
			SparkSession spark ) {

		// Distance UDF is only exception to GeomFunction interface since it
		// returns Double
		spark.udf().register(
				"GeomDistance",
				geomDistanceInstance,
				DataTypes.DoubleType);

		spark.udf().register(
				"GeomFromWKT",
				geomWKTInstance,
				GeoWaveSpatialEncoders.geometryUDT);

		// Register all UDF functions from RegistrySPI
		UDFNameAndConstructor[] supportedUDFs = UDFRegistrySPI.getSupportedUDFs();
		for (int iUDF = 0; iUDF < supportedUDFs.length; iUDF += 1) {
			UDFNameAndConstructor udf = supportedUDFs[iUDF];
			GeomFunction funcInstance = udf.getPredicateConstructor().get();

			spark.udf().register(
					funcInstance.getRegisterName(),
					funcInstance,
					DataTypes.BooleanType);
		}
	}
}
