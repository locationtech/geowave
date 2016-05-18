package mil.nga.giat.geowave.adapter.raster.operations.options;

import com.beust.jcommander.IStringConverter;

/**
 * This class will ensure that the hdfs parameter is in the correct format.
 */
public class HdfsHostPortConverter implements
		IStringConverter<String>
{
	@Override
	public String convert(
			String hdfsHostPort ) {
		if (!hdfsHostPort.contains("://")) {
			hdfsHostPort = "hdfs://" + hdfsHostPort;
		}
		return hdfsHostPort;
	}

}
