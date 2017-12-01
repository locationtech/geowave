package mil.nga.giat.geowave.analytic.spark;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.store.query.ScaledTemporalRange;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class GeoWaveRDD
{
	private static Logger LOGGER = LoggerFactory.getLogger(GeoWaveRDD.class);

	public static JavaPairRDD<GeoWaveInputKey, SimpleFeature> rddForSimpleFeatures(
			SparkContext sc,
			DataStorePluginOptions storeOptions )
			throws IOException {
		return rddForSimpleFeatures(
				sc,
				storeOptions,
				null,
				null,
				-1,
				-1);
	}

	public static JavaPairRDD<GeoWaveInputKey, SimpleFeature> rddForSimpleFeatures(
			SparkContext sc,
			DataStorePluginOptions storeOptions,
			DistributableQuery query )
			throws IOException {
		return rddForSimpleFeatures(
				sc,
				storeOptions,
				query,
				new QueryOptions(),
				-1,
				-1);
	}

	public static JavaPairRDD<GeoWaveInputKey, SimpleFeature> rddForSimpleFeatures(
			SparkContext sc,
			DataStorePluginOptions storeOptions,
			DistributableQuery query,
			QueryOptions queryOptions )
			throws IOException {
		return rddForSimpleFeatures(
				sc,
				storeOptions,
				query,
				queryOptions,
				-1,
				-1);
	}

	public static JavaPairRDD<GeoWaveInputKey, SimpleFeature> rddForSimpleFeatures(
			SparkContext sc,
			DataStorePluginOptions storeOptions,
			DistributableQuery query,
			QueryOptions queryOptions,
			int minSplits,
			int maxSplits )
			throws IOException {
		Configuration conf = new Configuration(
				sc.hadoopConfiguration());

		GeoWaveInputFormat.setStoreOptions(
				conf,
				storeOptions);

		if (query != null) {
			GeoWaveInputFormat.setQuery(
					conf,
					query);
		}

		if (queryOptions != null) {
			GeoWaveInputFormat.setQueryOptions(
					conf,
					queryOptions);
		}

		if (minSplits > -1) {
			GeoWaveInputFormat.setMinimumSplitCount(
					conf,
					minSplits);
			GeoWaveInputFormat.setMaximumSplitCount(
					conf,
					maxSplits);
		}
		else {
			int defaultSplitsSpark = sc.getConf().getInt(
					"spark.default.parallelism",
					-1);
			// Attempt to grab default partition count for spark and split data
			// along that.
			// Otherwise just fallback to default according to index strategy
			if (defaultSplitsSpark != -1) {
				GeoWaveInputFormat.setMinimumSplitCount(
						conf,
						defaultSplitsSpark);
				GeoWaveInputFormat.setMaximumSplitCount(
						conf,
						defaultSplitsSpark);
			}
		}

		RDD<Tuple2<GeoWaveInputKey, SimpleFeature>> rdd = sc.newAPIHadoopRDD(
				conf,
				GeoWaveInputFormat.class,
				GeoWaveInputKey.class,
				SimpleFeature.class);

		JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = JavaPairRDD.fromRDD(
				rdd,
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(GeoWaveInputKey.class),
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(SimpleFeature.class));

		return javaRdd;
	}

	/**
	 * Translate a set of objects in a JavaRDD to SimpleFeatures and push to
	 * GeoWave
	 * 
	 * @throws IOException
	 */
	public static void writeFeaturesToGeoWave(
			SparkContext sc,
			PrimaryIndex index,
			DataStorePluginOptions outputStoreOptions,
			FeatureDataAdapter adapter,
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> inputRDD )
			throws IOException {

		writeToGeoWave(
				sc,
				index,
				outputStoreOptions,
				adapter,
				inputRDD.values());
	}

	/**
	 * Translate a set of objects in a JavaRDD to a provided type and push to
	 * GeoWave
	 * 
	 * @throws IOException
	 */
	public static void writeToGeoWave(SparkContext sc,
	                                    PrimaryIndex index,
	                                    DataStorePluginOptions outputStoreOptions,
	                                    DataAdapter adapter,
	                                    JavaRDD<SimpleFeature> inputRDD) throws IOException{

	    //setup the configuration and the output format
	    Configuration conf = new org.apache.hadoop.conf.Configuration(sc.hadoopConfiguration());

	    GeoWaveOutputFormat.setStoreOptions(conf, outputStoreOptions);
	    GeoWaveOutputFormat.addIndex(conf, index);
	    GeoWaveOutputFormat.addDataAdapter(conf, adapter);


	    //create the job
	    Job job = new Job(conf);
	    job.setOutputKeyClass(GeoWaveOutputKey.class);
	    job.setOutputValueClass(SimpleFeature.class);
	    job.setOutputFormatClass(GeoWaveOutputFormat.class);

	    // broadcast byte ids
	    ClassTag<ByteArrayId> byteTag = scala.reflect.ClassTag$.MODULE$.apply(ByteArrayId.class);
	    Broadcast<ByteArrayId> adapterId = sc.broadcast(adapter.getAdapterId(), byteTag );
	    Broadcast<ByteArrayId> indexId = sc.broadcast(index.getId(), byteTag);

	    //map to a pair containing the output key and the output value
	    inputRDD.mapToPair(feat -> new Tuple2<GeoWaveOutputKey,SimpleFeature>(new GeoWaveOutputKey(adapterId.value(), indexId.value()),feat))
	    .saveAsNewAPIHadoopDataset(job.getConfiguration());
	  }

	public static JavaRDD<SimpleFeature> rddFeatures(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD ) {
		return pairRDD.values();
	}

	public static JavaRDD<Vector> rddFeatureVectors(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD ) {
		return rddFeatureVectors(
				pairRDD,
				null,
				null);
	}

	public static JavaRDD<Vector> rddFeatureVectors(
			final JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD,
			final String timeField,
			final ScaledTemporalRange scaledRange ) {
		JavaRDD<Vector> vectorRDD = pairRDD.values().map(
				feature -> {
					Point centroid = ((Geometry) feature.getDefaultGeometry()).getCentroid();

					int numValues = 2;
					Date time = null;

					if (timeField != null) {
						// if this is a ranged schema, we have to take the
						// midpoint
						if (timeField.contains(
								"|")) {
							int pipeIndex = timeField.indexOf(
									"|");
							String startField = timeField.substring(
									0,
									pipeIndex);
							String endField = timeField.substring(
									pipeIndex + 1);

							Date start = (Date) feature.getAttribute(
									startField);
							Date end = (Date) feature.getAttribute(
									endField);

							long halfDur = (end.getTime() - start.getTime()) / 2;

							time = new Date(
									start.getTime() + halfDur);
						}
						else {
							time = (Date) feature.getAttribute(
									timeField);
						}

						if (time != null) {
							numValues++;
						}
					}

					double[] values = new double[numValues];
					values[0] = centroid.getX();
					values[1] = centroid.getY();

					if (time != null) {
						values[2] = scaledRange.timeToValue(
								time);
					}

					return Vectors.dense(
							values);
				});

		return vectorRDD;
	}

	public static JavaRDD<Point> rddFeatureCentroids(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD ) {
		JavaRDD<Point> centroids = pairRDD.values().map(
				feature -> {
					Geometry geom = (Geometry) feature.getDefaultGeometry();
					return geom.getCentroid();
				});

		return centroids;
	}
}
