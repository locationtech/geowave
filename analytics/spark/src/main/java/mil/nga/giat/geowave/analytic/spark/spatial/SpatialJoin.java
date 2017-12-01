package mil.nga.giat.geowave.analytic.spark.spatial;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public interface SpatialJoin extends
		Serializable
{
	void join(
			SparkSession spark,
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftRDD,
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightRDD,
			GeomFunction predicate,
			NumericIndexStrategy indexStrategy )
			throws InterruptedException,
			ExecutionException;
}