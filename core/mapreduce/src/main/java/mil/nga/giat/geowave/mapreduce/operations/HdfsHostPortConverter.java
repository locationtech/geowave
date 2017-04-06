package mil.nga.giat.geowave.mapreduce.operations;

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
