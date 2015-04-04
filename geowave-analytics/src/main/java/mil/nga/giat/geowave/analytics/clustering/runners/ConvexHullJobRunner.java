package mil.nga.giat.geowave.analytics.clustering.runners;

import java.util.Set;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveJobRunner;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.clustering.mapreduce.ConvexHullMapReduce;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.HullParameters;
import mil.nga.giat.geowave.analytics.parameters.MapReduceParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.IndependentJobRunner;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.NumericIndexStrategyFactory.DataType;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.CustomIdIndex;
import mil.nga.giat.geowave.store.index.DimensionalityType;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.feature.type.BasicFeatureTypes;

/**
 * 

 */
public class ConvexHullJobRunner extends
		GeoWaveJobRunner implements
		MapReduceJobRunner,
		IndependentJobRunner
{

	private String projectionDataTypeId = "hull_polygon";
	private String indexId = null;
	private Path inputHDFSPath;
	private int reducerCount = 4;
	private String namespaceURI;

	public ConvexHullJobRunner() {}

	public void setIndexId(
			final String indexId ) {
		this.indexId = indexId;
	}

	public void setInputHDFSPath(
			final Path inputHDFSPath ) {
		this.inputHDFSPath = inputHDFSPath;
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {

		job.setJobName("GeoWave Convex Hull (" + namespace + ")");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(ConvexHullMapReduce.ConvexHullMap.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setReducerClass(ConvexHullMapReduce.ConvexHullReducer.class);
		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(Object.class);
		job.setNumReduceTasks(reducerCount);

		FileInputFormat.setInputPaths(
				job,
				inputHDFSPath);

		addDataAdapter(getAdapter());
		addIndex(getIndex());
	}

	private DataAdapter<?> getAdapter()
			throws Exception {
		final AccumuloOperations operations = new BasicAccumuloOperations(
				zookeeper,
				instance,
				user,
				password,
				namespace);

		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				operations);

		DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
				projectionDataTypeId));

		if (adapter == null) {
			adapter = AnalyticFeature.createGeometryFeatureAdapter(
					projectionDataTypeId,
					new String[0],
					namespaceURI,
					ClusteringUtils.CLUSTERING_CRS);
			adapterStore.addAdapter(adapter);
		}
		return adapter;

	}

	private Index getIndex()
			throws Exception {
		final AccumuloOperations operations = new BasicAccumuloOperations(
				zookeeper,
				instance,
				user,
				password,
				namespace);

		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);

		Index index = indexStore.getIndex(new ByteArrayId(
				indexId));
		if (index == null) {
			index = new CustomIdIndex(
					IndexType.SPATIAL_VECTOR.createDefaultIndexStrategy(),
					IndexType.SPATIAL_VECTOR.getDefaultIndexModel(),
					DimensionalityType.SPATIAL_TEMPORAL,
					DataType.VECTOR,
					new ByteArrayId(
							indexId));
			indexStore.addIndex(index);

		}
		return index;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		projectionDataTypeId = runTimeProperties.storeIfEmpty(
				HullParameters.Hull.DATA_TYPE_ID,
				"convex_hull").toString();
		namespaceURI = runTimeProperties.storeIfEmpty(
				HullParameters.Hull.DATA_NAMESPACE_URI,
				BasicFeatureTypes.DEFAULT_NAMESPACE).toString();
		indexId = runTimeProperties.getPropertyAsString(
				HullParameters.Hull.INDEX_ID,
				runTimeProperties.getPropertyAsString(CentroidParameters.Centroid.INDEX_ID));
		runTimeProperties.storeIfEmpty(
				HullParameters.Hull.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);
		runTimeProperties.storeIfEmpty(
				HullParameters.Hull.PROJECTION_CLASS,
				SimpleFeatureProjection.class);
		RunnerUtils.setParameter(
				config,
				ConvexHullMapReduce.class,
				runTimeProperties,
				new ParameterEnum[] {
					HullParameters.Hull.WRAPPER_FACTORY_CLASS,
					HullParameters.Hull.PROJECTION_CLASS,
					HullParameters.Hull.DATA_TYPE_ID,
					HullParameters.Hull.INDEX_ID
				});
		CentroidManagerGeoWave.setParameters(
				config,
				runTimeProperties);
		NestedGroupCentroidAssignment.setParameters(
				config,
				runTimeProperties);
		// getting group from next level, now that the prior level is complete
		NestedGroupCentroidAssignment.setZoomLevel(
				config,
				runTimeProperties.getPropertyAsInt(
						CentroidParameters.Centroid.ZOOM_LEVEL,
						1) + 1);

		return ToolRunner.run(
				config,
				this,
				runTimeProperties.toGeoWaveRunnerArguments());
	}

	@Override
	public void fillOptions(
			Set<Option> options ) {
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
		NestedGroupCentroidAssignment.fillOptions(options);

		HullParameters.fillOptions(
				options,
				new HullParameters.Hull[] {
					HullParameters.Hull.WRAPPER_FACTORY_CLASS,
					HullParameters.Hull.PROJECTION_CLASS,
					HullParameters.Hull.DATA_TYPE_ID,
					HullParameters.Hull.DATA_NAMESPACE_URI,
					HullParameters.Hull.INDEX_ID
				});
	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				MapReduceJobController.getConfiguration(runTimeProperties),
				runTimeProperties);
	}

}