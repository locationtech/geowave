package mil.nga.giat.geowave.analytic.param;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import mil.nga.giat.geowave.analytic.PropertyManagement;

public interface ParameterHelper<T> extends
		Serializable
{
	public Class<T> getBaseClass();

	public T getValue(
			PropertyManagement propertyManagement );

	public void setValue(
			PropertyManagement propertyManagement,
			T value );

	public void setValue(
			Configuration config,
			Class<?> scope,
			T value );

	public T getValue(
			JobContext context,
			Class<?> scope,
			T defaultValue );
}
