package mil.nga.giat.geowave.analytics.tools.mapreduce;

import java.util.Set;

import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public class GeoWaveOutputFormatConfiguration implements
		FormatConfiguration
{
	/**
	 * Captures the state, but the output format is flexible enough to deal with
	 * both.
	 * 
	 */
	protected boolean isDataWritable = false;

	@Override
	public void setup(
			PropertyManagement runTimeProperties,
			Configuration configuration )
			throws Exception {
		GeoWaveOutputFormat.setAccumuloOperationsInfo(
				configuration,
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ZOOKEEKER,
						"localhost:2181"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_INSTANCE,
						"miniInstance"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_USER,
						"root"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_PASSWORD,
						"password"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_NAMESPACE,
						"undefined"));

	}

	@Override
	public Class<?> getFormatClass() {
		return GeoWaveOutputFormat.class;
	}

	@Override
	public boolean isDataWritable() {
		return isDataWritable;
	}

	@Override
	public void setDataIsWritable(
			boolean isWritable ) {
		isDataWritable = isWritable;
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
					GlobalParameters.Global.ACCUMULO_NAMESPACE
				});
	}
}
