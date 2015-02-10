package mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.kmeans.mapreduce.KMeansDistortionMapReduce;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.ClusteringParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.JumpParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.mapreduce.CountofDoubleWritable;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;


//@formatter:off
/*if[ACCUMULO_1.5.1]
 else[ACCUMULO_1.5.1]*/
import org.apache.accumulo.core.client.ClientConfiguration;
/*end[ACCUMULO_1.5.1]*/
//@formatter:on
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


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
		Configured implements
		Tool,
		MapReduceJobRunner
{
	private int reducerCount = 8;
	private int k = 1;
	private Path inputHDFSPath;
	private String distortationTableName;

	public KMeansDistortionJobRunner() {

	}

	public void setReducerCount(
			final int reducerCount ) {
		this.reducerCount = reducerCount;
	}

	public void setCentroidsCount(
			final int k ) {
		this.k = k;
	}

	public void setDistortationTableName(
			final String distortationTableName ) {
		this.distortationTableName = distortationTableName;
	}

	public void setInputHDFSPath(
			final Path inputHDFSPath ) {
		this.inputHDFSPath = inputHDFSPath;
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		final Configuration conf = super.getConf();
		final Job job = new Job(
				conf);
		final String zookeeper = args[0];
		final String instance = args[1];
		final String user = args[2];
		final String password = args[3];
		final String namespace = args[4];

		job.setJarByClass(this.getClass());

		job.setJobName("GeoWave K-Means Distortion (" + namespace + ")");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(KMeansDistortionMapReduce.KMeansDistortionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CountofDoubleWritable.class);
		job.setReducerClass(KMeansDistortionMapReduce.KMeansDistortionReduce.class);
		job.setCombinerClass(KMeansDistortionMapReduce.KMeansDistorationCombiner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setNumReduceTasks(reducerCount);
		// extends wait time to one hour (default: 600 seconds)
		final long milliSeconds = 1000 * 60 * 60;
		job.getConfiguration().setLong(
				"mapred.task.timeout",
				milliSeconds);

		RunnerUtils.setParameter(
				job.getConfiguration(),
				KMeansDistortionMapReduce.class,
				new Object[] {
					new Integer(
							k)
				},
				new ParameterEnum[] {
					JumpParameters.Jump.COUNT_OF_CENTROIDS
				});

		FileInputFormat.setInputPaths(
				job,
				inputHDFSPath);

		// Required since the Mapper uses the input format parameters to lookup
		// the adapter
		GeoWaveInputFormat.setAccumuloOperationsInfo(
				job.getConfiguration(),
				zookeeper,
				instance,
				user,
				password,
				namespace);

		GeoWaveConfiguratorBase.setZookeeperUrl(
				KMeansDistortionMapReduce.class,
				job.getConfiguration(),
				zookeeper);
		GeoWaveConfiguratorBase.setInstanceName(
				KMeansDistortionMapReduce.class,
				job.getConfiguration(),
				instance);
		GeoWaveConfiguratorBase.setUserName(
				KMeansDistortionMapReduce.class,
				job.getConfiguration(),
				user);
		GeoWaveConfiguratorBase.setPassword(
				KMeansDistortionMapReduce.class,
				job.getConfiguration(),
				password);
		GeoWaveConfiguratorBase.setTableNamespace(
				KMeansDistortionMapReduce.class,
				job.getConfiguration(),
				namespace);

		final AuthenticationToken authToken = new PasswordToken(
				password.getBytes());
		// set up AccumuloOutputFormat
		AccumuloOutputFormat.setConnectorInfo(
				job,
				user,
				authToken);

		// @formatter:off
		/* if[ACCUMULO_1.5.1]
		AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeeper);
		else[ACCUMULO_1.5.1]*/
		final ClientConfiguration config = new ClientConfiguration().withZkHosts(
				zookeeper).withInstance(
				instance);
		AccumuloOutputFormat.setZooKeeperInstance(
				job,
				config);
		/* end[ACCUMULO_1.5.1] */
		// @formatter:on

		AccumuloOutputFormat.setDefaultTableName(
				job,
				distortationTableName);
		AccumuloOutputFormat.setCreateTables(
				job,
				true);

		final boolean jobSuccess = job.waitForCompletion(true);

		return (jobSuccess) ? 0 : 1;
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
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_NAMESPACE),
				runTimeProperties.getProperty(
						CentroidParameters.Centroid.DISTORTION_TABLE_NAME,
						"KmeansDistortion"));
		reducerCount = Math.min(
				runTimeProperties.getPropertyAsInt(
						ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
						16),
				reducerCount);
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

		return ToolRunner.run(
				config,
				this,
				runTimeProperties.toGeoWaveRunnerArguments());
	}
}