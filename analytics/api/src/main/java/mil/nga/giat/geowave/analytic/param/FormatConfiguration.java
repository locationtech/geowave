package mil.nga.giat.geowave.analytic.param;

import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public interface FormatConfiguration
{
	public void setup(
			PropertyManagement runTimeProperties,
			Configuration configuration )
			throws Exception;

	public Class<?> getFormatClass();

	/**
	 * If the format supports only one option, then 'setting' the data has no
	 * effect.
	 * 
	 * @return true if the data is a Hadoop Writable or an POJO.
	 * 
	 */

	public boolean isDataWritable();

	public void setDataIsWritable(
			boolean isWritable );

	public void fillOptions(
			Set<Option> options );
}
