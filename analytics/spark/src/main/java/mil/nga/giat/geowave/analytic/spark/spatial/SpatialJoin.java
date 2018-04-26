package mil.nga.giat.geowave.analytic.spark.spatial;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import org.apache.spark.sql.SparkSession;
import mil.nga.giat.geowave.analytic.spark.GeoWaveIndexedRDD;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;

public interface SpatialJoin extends
		Serializable
{
	void join(
			SparkSession spark,
			GeoWaveIndexedRDD leftRDD,
			GeoWaveIndexedRDD rightRDD,
			GeomFunction predicate )
			throws InterruptedException,
			ExecutionException;

	boolean supportsJoin(
			NumericIndexStrategy indexStrategy );

	NumericIndexStrategy createDefaultStrategy(
			NumericIndexStrategy indexStrategy );
}