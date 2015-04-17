package mil.nga.giat.geowave.analytics.clustering.runners;

import java.util.Set;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
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
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GeoWaveOutputFormatConfiguration;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.NumericIndexStrategyFactory.DataType;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.CustomIdIndex;
import mil.nga.giat.geowave.store.index.DimensionalityType;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.geotools.feature.type.BasicFeatureTypes;

/**
 * 

 */
public class ConvexHullJobRunner extends
		GeoWaveAnalyticJobRunner
{

	private int zoomLevel = 1;

	public ConvexHullJobRunner() {
		super.setOutputFormatConfiguration(new GeoWaveOutputFormatConfiguration());
	}

	public void setZoomLevel(
			int zoomLevel ) {
		this.zoomLevel = zoomLevel;
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {
		job.setMapperClass(ConvexHullMapReduce.ConvexHullMap.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setReducerClass(ConvexHullMapReduce.ConvexHullReducer.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(Object.class);
	}

	private DataAdapter<?> getAdapter(
			final PropertyManagement runTimeProperties )
			throws Exception {

		final String projectionDataTypeId = runTimeProperties.storeIfEmpty(
				HullParameters.Hull.DATA_TYPE_ID,
				"convex_hull").toString();

		final AdapterStore adapterStore = super.getAdapterStore(runTimeProperties);

		DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
				projectionDataTypeId));

		if (adapter == null) {
			final String namespaceURI = runTimeProperties.storeIfEmpty(
					HullParameters.Hull.DATA_NAMESPACE_URI,
					BasicFeatureTypes.DEFAULT_NAMESPACE).toString();
			adapter = AnalyticFeature.createGeometryFeatureAdapter(
					projectionDataTypeId,
					new String[0],
					namespaceURI,
					ClusteringUtils.CLUSTERING_CRS);
			adapterStore.addAdapter(adapter);
		}
		return adapter;

	}

	private void checkIndex(
			final PropertyManagement runTimeProperties )
			throws Exception {

		final String indexId = runTimeProperties.getPropertyAsString(
				HullParameters.Hull.INDEX_ID,
				runTimeProperties.getPropertyAsString(CentroidParameters.Centroid.INDEX_ID));

		final IndexStore indexStore = super.getIndexStore(runTimeProperties);

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
	}

	public Class<?> getScope() {
		return ConvexHullMapReduce.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				HullParameters.Hull.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);
		runTimeProperties.storeIfEmpty(
				HullParameters.Hull.PROJECTION_CLASS,
				SimpleFeatureProjection.class);
		RunnerUtils.setParameter(
				config,
				getScope(),
				runTimeProperties,
				new ParameterEnum[] {
					HullParameters.Hull.WRAPPER_FACTORY_CLASS,
					HullParameters.Hull.PROJECTION_CLASS,
					HullParameters.Hull.DATA_TYPE_ID,
					HullParameters.Hull.INDEX_ID
				});
		setReducerCount(runTimeProperties.getPropertyAsInt(
				HullParameters.Hull.REDUCER_COUNT,
				4));
		CentroidManagerGeoWave.setParameters(
				config,
				runTimeProperties);
		NestedGroupCentroidAssignment.setParameters(
				config,
				runTimeProperties);

		int localZoomLevel = runTimeProperties.getPropertyAsInt(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				zoomLevel);
		// getting group from next level, now that the prior level is complete
		NestedGroupCentroidAssignment.setZoomLevel(
				config,
				localZoomLevel + 1);

		addDataAdapter(
				config,
				getAdapter(runTimeProperties));
		checkIndex(runTimeProperties);

		return super.run(
				config,
				runTimeProperties);
	}

	@Override
	public void fillOptions(
			Set<Option> options ) {
		super.fillOptions(options);

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
					HullParameters.Hull.REDUCER_COUNT,
					HullParameters.Hull.DATA_TYPE_ID,
					HullParameters.Hull.DATA_NAMESPACE_URI,
					HullParameters.Hull.INDEX_ID
				});
	}

}