package mil.nga.giat.geowave.analytic.spark;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.store.query.ScaledTemporalRange;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class RDDUtils
{

	private static Logger LOGGER = LoggerFactory.getLogger(RDDUtils.class);

	/**
	 * Translate a set of objects in a JavaRDD to SimpleFeatures and push to
	 * GeoWave
	 * 
	 * @throws IOException
	 */
	public static void writeRDDToGeoWave(
			SparkContext sc,
			PrimaryIndex index,
			DataStorePluginOptions outputStoreOptions,
			FeatureDataAdapter adapter,
			GeoWaveRDD inputRDD )
			throws IOException {
		if (!inputRDD.isLoaded()) {
			LOGGER.error("Must provide a loaded RDD.");
			return;
		}

		writeToGeoWave(
				sc,
				index,
				outputStoreOptions,
				adapter,
				inputRDD.getRawRDD().values());
	}

	public static void writeRDDToGeoWave(
			SparkContext sc,
			PrimaryIndex[] indices,
			DataStorePluginOptions outputStoreOptions,
			FeatureDataAdapter adapter,
			GeoWaveRDD inputRDD )
			throws IOException {
		if (!inputRDD.isLoaded()) {
			LOGGER.error("Must provide a loaded RDD.");
			return;
		}

		for (int iStrategy = 0; iStrategy < indices.length; iStrategy += 1) {
			writeToGeoWave(
					sc,
					indices[iStrategy],
					outputStoreOptions,
					adapter,
					inputRDD.getRawRDD().values());
		}
	}

	public static JavaRDD<Point> rddFeatureCentroids(
			GeoWaveRDD inputRDD ) {
		if(!inputRDD.isLoaded()) {
			LOGGER.error("Must provide a loaded RDD.");
			return null;
		}
		JavaRDD<Point> centroids = inputRDD.getRawRDD().values().map(
				feature -> {
					Geometry geom = (Geometry) feature.getDefaultGeometry();
					return geom.getCentroid();
				});

		return centroids;
	}

	public static JavaRDD<Vector> rddFeatureVectors(
			GeoWaveRDD inputRDD ) {

		return rddFeatureVectors(
				inputRDD,
				null,
				null);
	}

	public static JavaRDD<Vector> rddFeatureVectors(
			final GeoWaveRDD inputRDD,
			final String timeField,
			final ScaledTemporalRange scaledRange ) {
		if(!inputRDD.isLoaded()) {
			LOGGER.error("Must provide a loaded RDD.");
			return null;
		}
		JavaRDD<Vector> vectorRDD = inputRDD.getRawRDD().values().map(
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

	/**
	 * Translate a set of objects in a JavaRDD to a provided type and push to
	 * GeoWave
	 * 
	 * @throws IOException
	 */
	private static void writeToGeoWave(SparkContext sc,
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

	public static Broadcast<? extends NumericIndexStrategy> broadcastIndexStrategy(
			SparkContext sc,
			NumericIndexStrategy indexStrategy ) {
		ClassTag<NumericIndexStrategy> indexClassTag = scala.reflect.ClassTag$.MODULE$.apply(indexStrategy.getClass());
		Broadcast<NumericIndexStrategy> broadcastStrategy = sc.broadcast(
				indexStrategy,
				indexClassTag);
		return broadcastStrategy;
	}
}
