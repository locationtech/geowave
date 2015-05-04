package mil.nga.giat.geowave.analytic.mapreduce.kmeans.runner;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.mapreduce.CountofDoubleWritable;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.kmeans.KMeansDistortionMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.JumpParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;

import mil.nga.giat.geowave.core.index.StringUtils;

import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

//@formatter:off
/*if[ACCUMULO_1.5.2]
 else[ACCUMULO_1.5.2]*/
import org.apache.accumulo.core.client.ClientConfiguration;
/*end[ACCUMULO_1.5.2]*/
//@formatter:on
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * 
 * Calculate the distortation.
 * 
 * See Catherine A. Sugar and Gareth M. James (2003).
 * "Finding the number of clusters in a data set: An information theoretic approach"
 * Journal of the American Statistical Association 98 (January): 750–763
 * 
 * 
 */
public class KMeansDistortionJobRunner extends
		GeoWaveAnalyticJobRunner
{
	private int k = 1;
	private String distortationTableName;

	public KMeansDistortionJobRunner() {
		setReducerCount(8);
	}

	public void setCentroidsCount(
			final int k ) {
		this.k = k;
	}

	public void setDistortationTableName(
			final String distortationTableName ) {
		this.distortationTableName = distortationTableName;
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {

		job.setMapperClass(KMeansDistortionMapReduce.KMeansDistortionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CountofDoubleWritable.class);
		job.setReducerClass(KMeansDistortionMapReduce.KMeansDistortionReduce.class);
		job.setCombinerClass(KMeansDistortionMapReduce.KMeansDistorationCombiner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		// extends wait time to 15 minutes (default: 600 seconds)
		final long milliSeconds = 1000 * 60 * 15;
		job.getConfiguration().setLong(
				"mapred.task.timeout",
				milliSeconds);

		RunnerUtils.setParameter(
				job.getConfiguration(),
				KMeansDistortionMapReduce.class,
				new Object[] {
					Integer.valueOf(k)
				},
				new ParameterEnum[] {
					JumpParameters.Jump.COUNT_OF_CENTROIDS
				});

		// Required since the Mapper uses the input format parameters to lookup
		// the adapter
		GeoWaveInputFormat.setAccumuloOperationsInfo(
				job.getConfiguration(),
				zookeeper,
				instanceName,
				userName,
				password,
				namespace);

		GeoWaveConfiguratorBase.setZookeeperUrl(
				KMeansDistortionMapReduce.class,
				job.getConfiguration(),
				zookeeper);
		GeoWaveConfiguratorBase.setInstanceName(
				KMeansDistortionMapReduce.class,
				job.getConfiguration(),
				instanceName);
		GeoWaveConfiguratorBase.setUserName(
				KMeansDistortionMapReduce.class,
				job.getConfiguration(),
				userName);
		GeoWaveConfiguratorBase.setPassword(
				KMeansDistortionMapReduce.class,
				job.getConfiguration(),
				password);
		GeoWaveConfiguratorBase.setTableNamespace(
				KMeansDistortionMapReduce.class,
				job.getConfiguration(),
				namespace);

		final AuthenticationToken authToken = new PasswordToken(
				password.getBytes(StringUtils.UTF8_CHAR_SET));
		// set up AccumuloOutputFormat
		AccumuloOutputFormat.setConnectorInfo(
				job,
				userName,
				authToken);

		// @formatter:off
		/* if[ACCUMULO_1.5.2]
		AccumuloOutputFormat.setZooKeeperInstance(job, instanceName, zookeeper);
		else[ACCUMULO_1.5.2]*/
		final ClientConfiguration config = new ClientConfiguration().withZkHosts(
				zookeeper).withInstance(
				instanceName);
		AccumuloOutputFormat.setZooKeeperInstance(
				job,
				config);
		/* end[ACCUMULO_1.5.2] */
		// @formatter:on

		AccumuloOutputFormat.setDefaultTableName(
				job,
				distortationTableName);
		AccumuloOutputFormat.setCreateTables(
				job,
				true);
	}

	@Override
	public Class<?> getScope() {
		return KMeansDistortionMapReduce.class;
	}

	public String getDistortationTableName() {
		return distortationTableName;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		distortationTableName = AccumuloUtils.getQualifiedTableName(
				runTimeProperties.getPropertyAsString(GlobalParameters.Global.ACCUMULO_NAMESPACE),
				runTimeProperties.getPropertyAsString(
						CentroidParameters.Centroid.DISTORTION_TABLE_NAME,
						"KmeansDistortion"));
		setReducerCount(runTimeProperties.getPropertyAsInt(
				ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
				super.getReducerCount()));
		RunnerUtils.setParameter(
				config,
				KMeansDistortionMapReduce.class,
				runTimeProperties,
				new ParameterEnum[] {
					CentroidParameters.Centroid.EXTRACTOR_CLASS,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS
				});

		NestedGroupCentroidAssignment.setParameters(
				config,
				runTimeProperties);

		return super.run(
				config,
				runTimeProperties);
	}
}