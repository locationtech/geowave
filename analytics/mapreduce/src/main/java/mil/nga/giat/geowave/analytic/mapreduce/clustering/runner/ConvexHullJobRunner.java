package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import java.util.Set;

import mil.nga.giat.geowave.analytic.Projection;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.ConvexHullMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.output.GeoWaveOutputKey;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;

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
			final int zoomLevel ) {
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

	@Override
	public Class<?> getScope() {
		return ConvexHullMapReduce.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				HullParameters.Hull.PROJECTION_CLASS,
				SimpleFeatureProjection.class);

		final Projection<?> projectionFunction = runTimeProperties.getClassInstance(
				HullParameters.Hull.PROJECTION_CLASS,
				Projection.class,
				SimpleFeatureProjection.class);

		projectionFunction.setup(
				runTimeProperties,
				config);

		runTimeProperties.storeIfEmpty(
				HullParameters.Hull.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);

		RunnerUtils.setParameter(
				config,
				getScope(),
				new Object[] {
					checkIndex(
							runTimeProperties,
							HullParameters.Hull.INDEX_ID,
							runTimeProperties.getPropertyAsString(
									CentroidParameters.Centroid.INDEX_ID,
									"hull_idx"))
				},
				new ParameterEnum[] {
					HullParameters.Hull.INDEX_ID
				});

		RunnerUtils.setParameter(
				config,
				getScope(),
				runTimeProperties,
				new ParameterEnum[] {
					HullParameters.Hull.WRAPPER_FACTORY_CLASS,
					HullParameters.Hull.PROJECTION_CLASS,
					HullParameters.Hull.DATA_TYPE_ID
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

		final int localZoomLevel = runTimeProperties.getPropertyAsInt(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				zoomLevel);
		// getting group from next level, now that the prior level is complete
		NestedGroupCentroidAssignment.setZoomLevel(
				config,
				localZoomLevel + 1);

		addDataAdapter(
				config,
				getAdapter(
						runTimeProperties,
						HullParameters.Hull.DATA_TYPE_ID,
						HullParameters.Hull.DATA_NAMESPACE_URI));

		return super.run(
				config,
				runTimeProperties);
	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
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