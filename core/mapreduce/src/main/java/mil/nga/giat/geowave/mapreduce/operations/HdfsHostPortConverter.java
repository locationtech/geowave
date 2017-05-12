package mil.nga.giat.geowave.mapreduce.operations;

import mil.nga.giat.geowave.core.cli.converters.GeoWaveBaseConverter;

/**
 * This class will ensure that the hdfs parameter is in the correct format.
 */
public class HdfsHostPortConverter extends
		GeoWaveBaseConverter<String>
{
	public HdfsHostPortConverter(
			String optionName ) {
		super(
				optionName);
	}

	@Override
	public String convert(
			String hdfsHostPort ) {
		if (!hdfsHostPort.contains("://")) {
			hdfsHostPort = "hdfs://" + hdfsHostPort;
		}
		return hdfsHostPort;
	}

	@Override
	public boolean isRequired() {
		return true;
	}
}