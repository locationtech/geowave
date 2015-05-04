package mil.nga.giat.geowave.analytic.mapreduce;

import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.InputParameters;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class SequenceFileInputFormatConfiguration implements
		FormatConfiguration
{

	final Path inputPath;

	public SequenceFileInputFormatConfiguration() {
		inputPath = null;
	}

	public SequenceFileInputFormatConfiguration(
			final Path inputPath ) {
		this.inputPath = inputPath;
	}

	@Override
	public void setup(
			final PropertyManagement runTimeProperties,
			final Configuration configuration )
			throws Exception {
		final Path localInputPath = inputPath == null ? runTimeProperties.getPropertyAsPath(InputParameters.Input.HDFS_INPUT_PATH) : inputPath;
		if (localInputPath != null) {
			configuration.set(
					"mapred.input.dir",
					localInputPath.toString());
		}

	}

	@Override
	public Class<?> getFormatClass() {
		return SequenceFileInputFormat.class;
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
		InputParameters.fillOptions(
				options,
				new InputParameters.Input[] {
					InputParameters.Input.HDFS_INPUT_PATH
				});
	}

}
