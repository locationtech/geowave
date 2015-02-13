package mil.nga.giat.geowave.analytics.spark;

import java.util.Iterator;
import java.util.Set;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.analytics.clustering.NearestNeighborsQueryTool;
import mil.nga.giat.geowave.analytics.parameters.ExtractParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.MapReduceParameters;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.mapreduce.HadoopOptions;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class GeowaveGraphRDD<InputType> extends
		GeowaveRDD
{

	protected static final Logger LOGGER = Logger.getLogger(GeowaveGraphRDD.class);

	public void doit(
			Configuration config ) {
		GeoWaveInputFormat.setIsOutputWritable(
				config,
				true);

		final SparkConf conf = new SparkConf().setAppName(
				"Simple Application").setMaster(
				"local");
		/*
		 * conf.set( "spark.serializer",
		 * "org.apache.spark.serializer.KryoSerializer"); conf.set(
		 * "spark.kryo.registrator", MyRegistrator.class.getName());
		 */
		final JavaSparkContext sc = new JavaSparkContext(
				conf);

		JavaPairRDD<GeoWaveInputKey, InputType> verticesData = sc.newAPIHadoopRDD(
				config,
				GeoWaveInputFormat.class,
				GeoWaveInputKey.class,
				Object.class);

		final NearestNeighborsQueryTool queryTool = new NearestNeighborsQueryTool<InputType>();
		final AdapterStore store = null;
		final EdgeRDD<Edge<Double>> edgeData = EdgeRDD.fromEdges(verticesData.flatMap(
				new FlatMapFunction<Tuple2<GeoWaveInputKey, InputType>, Edge<Double>>() {

					@Override
					public Iterable<Edge<Double>> call(
							final Tuple2<GeoWaveInputKey, InputType> t )
							throws Exception {
						return new Iterable<Edge<Double>>() {
							public Iterator<Edge<Double>> iterator() {
								final CloseableIterator<InputType> it = queryTool.getNeighbors(
										t._1.getAdapterId(),
										t._2);
								return new Iterator<Edge<Double>>() {

									@Override
									public boolean hasNext() {
										if (!it.hasNext()) {
											it.close();
											return false;
										}
										return true;
									}

									@SuppressWarnings("unchecked")
									@Override
									public Edge<Double> next() {
										InputType entity = it.next();
										ByteArrayId id = ((DataAdapter<InputType>) store.getAdapter(t._1.getAdapterId())).getDataId(entity);
										new Edge<Double>(
												id.hashCode(),
												t._1.getDataId().hashCode(),
												0.0);
									}

									@Override
									public void remove() {}
								};
							}
						};
					}
				}).cache(),Double.class,Class.class);

		Graph<GeoWaveInputKey, InputType> graph = null;

		long j = geoData.mapToPair(
				new PairFunction<Tuple2<GeoWaveInputKey, Writable>, byte[], Writable>() {
					@SuppressWarnings({
						"rawtypes",
						"unchecked"
					})
					@Override
					public Tuple2<byte[], Writable> call(
							Tuple2<GeoWaveInputKey, Writable> arg0 )
							throws Exception {
						return new Tuple2(
								arg0._1.getAdapterId().getBytes(),
								arg0._2);
					}
				}).groupByKey().count();
		System.out.println(j);
		sc.close();
	}

	@Override
	public void fillOptions(
			Set<Option> options ) {
		ExtractParameters.fillOptions(
				options,
				new ExtractParameters.Extract[] {
					ExtractParameters.Extract.REDUCER_COUNT,
					ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
					ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS,
					ExtractParameters.Extract.INDEX_ID,
					ExtractParameters.Extract.ADAPTER_ID,
					ExtractParameters.Extract.MIN_INPUT_SPLIT,
					ExtractParameters.Extract.MAX_INPUT_SPLIT,
					ExtractParameters.Extract.QUERY
				});
		GlobalParameters.fillOptions(
				options,
				new GlobalParameters.Global[] {
					GlobalParameters.Global.ZOOKEEKER,
					GlobalParameters.Global.ACCUMULO_INSTANCE,
					GlobalParameters.Global.ACCUMULO_PASSWORD,
					GlobalParameters.Global.ACCUMULO_USER,
					GlobalParameters.Global.ACCUMULO_NAMESPACE,
					GlobalParameters.Global.BATCH_ID
				});

		MapReduceParameters.fillOptions(options);

	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				new HadoopOptions(
						runTimeProperties).getConfiguration(),
				runTimeProperties);
	}

}
