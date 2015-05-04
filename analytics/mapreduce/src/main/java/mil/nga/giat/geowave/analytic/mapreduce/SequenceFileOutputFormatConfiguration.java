package mil.nga.giat.geowave.analytic.mapreduce;

import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.OutputParameters;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SequenceFileOutputFormatConfiguration implements
		FormatConfiguration
{

	final Path outputPath;

	public SequenceFileOutputFormatConfiguration() {
		outputPath = null;
	}

	public SequenceFileOutputFormatConfiguration(
			final Path outputPath ) {
		this.outputPath = outputPath;
	}

	@Override
	public void setup(
			final PropertyManagement runTimeProperties,
			final Configuration configuration )
			throws Exception {

		final Path localOutputPath = outputPath == null ? runTimeProperties.getPropertyAsPath(OutputParameters.Output.HDFS_OUTPUT_PATH) : outputPath;
		if (localOutputPath != null) {
			configuration.set(
					"mapred.output.dir",
					localOutputPath.toString());
		}

	}

	@Override
	public Class<?> getFormatClass() {
		return SequenceFileOutputFormat.class;
	}

	@Override
	public boolean isDataWritable() {
		return true;
	}

	@Override
	public void setDataIsWritable(
			final boolean isWritable ) {

	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		OutputParameters.fillOptions(
				options,
				new OutputParameters.Output[] {
					OutputParameters.Output.HDFS_OUTPUT_PATH
				});
	}

}
