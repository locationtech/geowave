package mil.nga.giat.geowave.analytics.tools;

import java.io.Serializable;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.index.ByteArrayUtils;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunnerUtils
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(RunnerUtils.class);

	public static final void setParameter(
			final Configuration config,
			final Class<?> clazz,
			final Object[] values,
			final ParameterEnum[] enums ) {
		int i = 0;
		for (final ParameterEnum configItem : enums) {
			final Object val = values[i++];
			if (val != null) {
				if (val instanceof Long) {
					config.setLong(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							((Long) val));
				}
				else if (val instanceof Double) {
					config.setDouble(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							((Double) val));
				}
				else if (val instanceof Boolean) {
					config.setBoolean(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							((Boolean) val));
				}
				else if (val instanceof Integer) {
					config.setInt(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							((Integer) val));
				}
				else if (val instanceof Class) {
					config.setClass(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							((Class) val),
							configItem.getBaseClass());
				}
				else {
					config.set(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							val.toString());
				}

			}
		}
	}

	public static final void setParameter(
			final Configuration config,
			final Class<?> clazz,
			final PropertyManagement pmt,
			final ParameterEnum[] enums ) {
		for (final ParameterEnum configItem : enums) {
			final Serializable val = pmt.get(configItem);
			if (val != null) {
				if (val instanceof Long) {
					config.setLong(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							((Long) val));
				}
				else if (val instanceof Double) {
					config.setDouble(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							((Double) val));
				}
				else if (val instanceof Boolean) {
					config.setBoolean(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							((Boolean) val));
				}
				else if (val instanceof Integer) {
					config.setInt(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							((Integer) val));
				}
				else if (val instanceof Class) {
					config.setClass(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							((Class) val),
							configItem.getBaseClass());
				}
				else if (val instanceof byte[]) {
					config.set(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							ByteArrayUtils.byteArrayToString((byte[]) val));
				}
				else {
					config.set(
							GeoWaveConfiguratorBase.enumToConfKey(
									clazz,
									configItem.self()),
							val.toString());
				}

			}
		}
	}
}
