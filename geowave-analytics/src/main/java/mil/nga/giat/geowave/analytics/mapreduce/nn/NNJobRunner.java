package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.util.Set;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.analytics.mapreduce.nn.NNMapReduce.PartitionDataWritable;
import mil.nga.giat.geowave.analytics.parameters.CommonParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.parameters.PartitionParameters;
import mil.nga.giat.geowave.analytics.parameters.PartitionParameters.Partition;
import mil.nga.giat.geowave.analytics.tools.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytics.tools.partitioners.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytics.tools.partitioners.Partitioner;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class NNJobRunner extends
		GeoWaveAnalyticJobRunner
{

	@Override
	public void configure(
			final Job job )
			throws Exception {
		job.setMapperClass(NNMapReduce.NNMapper.class);
		job.setReducerClass(NNMapReduce.NNSimpleFeatureIDOutputReducer.class);
		job.setMapOutputKeyClass(PartitionDataWritable.class);
		job.setMapOutputValueClass(AdapterWithObjectWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setSpeculativeExecution(false);

		GeoWaveInputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instanceName,
				userName,
				password,
				namespace);

	}

	@Override
	public Class<?> getScope() {
		return NNMapReduce.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		Partitioner<?> partitioner = runTimeProperties.getClassInstance(
				Partition.PARTITIONER_CLASS,
				Partitioner.class,
				OrthodromicDistancePartitioner.class);

		partitioner.setup(
				runTimeProperties,
				config);

		RunnerUtils.setParameter(
				config,
				getScope(),
				runTimeProperties,
				new ParameterEnum[] {
					Partition.PARTITIONER_CLASS,
					Partition.PARTITION_DISTANCE,
					Partition.MAX_MEMBER_SELECTION,
					CommonParameters.Common.DISTANCE_FUNCTION_CLASS
				});

		return super.run(
				config,
				runTimeProperties);

	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		super.fillOptions(options);
		PartitionParameters.fillOptions(
				options,
				new PartitionParameters.Partition[] {
					Partition.PARTITIONER_CLASS,
					Partition.PARTITION_DISTANCE,
					Partition.MAX_MEMBER_SELECTION
				});

		CommonParameters.fillOptions(
				options,
				new CommonParameters.Common[] {
					CommonParameters.Common.DISTANCE_FUNCTION_CLASS
				});
	}

}
