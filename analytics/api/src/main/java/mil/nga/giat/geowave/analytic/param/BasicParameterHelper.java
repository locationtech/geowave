package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicParameterHelper implements
		ParameterHelper<Object>
{
	final static Logger LOGGER = LoggerFactory.getLogger(BasicParameterHelper.class);
	private final ParameterEnum<?> parent;
	private final Class<Object> baseClass;
	private final Option[] options;

	public BasicParameterHelper(
			final ParameterEnum<?> parent,
			final Class<Object> baseClass,
			final String name,
			final String description,
			final boolean hasArg ) {
		this.baseClass = baseClass;
		this.parent = parent;
		options = new Option[] {
			newOption(
					parent,
					name,
					description,
					hasArg)
		};
	}

	@Override
	public Class<Object> getBaseClass() {
		return baseClass;
	}

	@Override
	public Option[] getOptions() {
		return options;
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final Object value ) {
		setParameter(
				config,
				scope,
				value,
				parent);
	}

	private static final void setParameter(
			final Configuration config,
			final Class<?> clazz,
			final Object val,
			final ParameterEnum configItem ) {
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
						((Class) val));
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

	private static final Option newOption(
			final ParameterEnum e,
			final String name,
			final String description,
			final boolean hasArg ) {
		return new Option(
				name,
				toPropertyName(e),
				hasArg,
				description);
	}

	private static final String toPropertyName(
			final ParameterEnum param ) {
		return param.getClass().getSimpleName().toLowerCase() + "-" + param.self().name().replace(
				'_',
				'-').toLowerCase();
	}

	@Override
	public Object getValue(
			final JobContext context,
			final Class<?> scope,
			final Object defaultValue ) {
		final ScopedJobConfiguration scopedConfig = new ScopedJobConfiguration(
				context.getConfiguration(),
				scope);
		if (baseClass.isAssignableFrom(Integer.class)) {
			return Integer.valueOf(scopedConfig.getInt(
					parent.self(),
					((Integer) defaultValue).intValue()));
		}
		else if (baseClass.isAssignableFrom(String.class)) {
			return scopedConfig.getString(
					parent.self(),
					defaultValue.toString());
		}
		else if (baseClass.isAssignableFrom(Double.class)) {
			return scopedConfig.getDouble(
					parent.self(),
					(Double) defaultValue);
		}
		else if (baseClass.isAssignableFrom(byte[].class)) {
			return scopedConfig.getBytes(parent.self());
		}
		else if ((defaultValue == null) || (defaultValue instanceof Class)) {
			try {
				return scopedConfig.getInstance(
						parent.self(),
						baseClass,
						(Class) defaultValue);
			}
			catch (InstantiationException | IllegalAccessException e) {
				LOGGER.error(
						"Unable to get instance from job context",
						e);
			}
		}
		return null;
	}

	@Override
	public CommandLineResult<Object> getValue(
			final Options allOptions,
			final CommandLine commandline ) {
		return new CommandLineResult<Object>(
				getValueInternal(
						allOptions,
						commandline));
	}

	private Object getValueInternal(
			final Options allOptions,
			final CommandLine commandline ) {
		if (baseClass.isAssignableFrom(Boolean.class)) {
			return commandline.hasOption(options[0].getOpt());
		}
		final String optionValueStr = commandline.getOptionValue(options[0].getOpt());
		if (optionValueStr == null) {
			return null;
		}
		if (baseClass.isAssignableFrom(Integer.class)) {
			return Integer.parseInt(optionValueStr);
		}
		else if (baseClass.isAssignableFrom(String.class)) {
			return optionValueStr;
		}
		else if (baseClass.isAssignableFrom(Double.class)) {
			return Double.parseDouble(optionValueStr);
		}
		else if (baseClass.isAssignableFrom(byte[].class)) {
			return ByteArrayUtils.byteArrayFromString(optionValueStr);
		}
		return null;
	}

	@Override
	public Object getValue(
			final PropertyManagement propertyManagement ) {
		try {
			return propertyManagement.getProperty(parent);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to deserialize property '" + parent.toString() + "'",
					e);
			return null;
		}
	}

	@Override
	public void setValue(
			final PropertyManagement propertyManagement,
			final Object value ) {
		propertyManagement.store(
				parent,
				value);
	}
}
