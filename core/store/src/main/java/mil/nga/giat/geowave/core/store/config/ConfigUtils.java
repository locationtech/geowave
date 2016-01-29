package mil.nga.giat.geowave.core.store.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;

public class ConfigUtils
{
	public static String cleanOptionName(
			String name ) {
		name = name.trim().toLowerCase().replaceAll(
				" ",
				"_");
		name = name.replaceAll(
				",",
				"");
		return name;
	}

	public static StringBuilder getOptions(
			final Collection<String> strs,
			final String prefixStr ) {

		final StringBuilder builder = new StringBuilder();
		for (final String str : strs) {
			if (builder.length() > 0) {
				builder.append(",");
			}
			else {
				builder.append(prefixStr);
			}
			builder.append(
					"'").append(
					cleanOptionName(str)).append(
					"'");
		}
		return builder;
	}

	public static StringBuilder getOptions(
			final Collection<String> strs ) {
		return getOptions(
				strs,
				"Options include: ");
	}

	@SuppressWarnings("unchecked")
	public static Map<String, String> valuesToStrings(
			final Map<String, Object> objectValues,
			final AbstractConfigOption<?>[] configOptions ) {
		final Map<String, String> stringValues = new HashMap<String, String>(
				objectValues.size());
		final Map<String, AbstractConfigOption<Object>> configOptionMap = new HashMap<String, AbstractConfigOption<Object>>();
		// first get a map of optionname to option
		for (final AbstractConfigOption<?> option : configOptions) {
			configOptionMap.put(
					option.getName(),
					(AbstractConfigOption<Object>) option);
		}
		for (final Entry<String, Object> objectValue : objectValues.entrySet()) {
			final AbstractConfigOption<Object> option = configOptionMap.get(objectValue.getKey());
			stringValues.put(
					objectValue.getKey(),
					option.valueToString(objectValue.getValue()));
		}
		return stringValues;
	}

	public static Map<String, Object> valuesFromStrings(
			final Map<String, String> stringValues ) {
		return valuesFromStrings(
				stringValues,
				GeoWaveStoreFinder.getAllOptions());
	}

	public static Map<String, Object> valuesFromStrings(
			final Map<String, String> stringValues,
			final AbstractConfigOption<?>[] configOptions ) {
		final Map<String, Object> objectValues = new HashMap<String, Object>(
				stringValues.size());
		final Map<String, AbstractConfigOption<?>> configOptionMap = new HashMap<String, AbstractConfigOption<?>>();
		// first get a map of optionname to option
		for (final AbstractConfigOption<?> option : configOptions) {
			configOptionMap.put(
					option.getName(),
					option);
		}
		for (final Entry<String, String> stringValue : stringValues.entrySet()) {
			final AbstractConfigOption<?> option = configOptionMap.get(stringValue.getKey());
			if (option != null) {
				objectValues.put(
						stringValue.getKey(),
						option.valueFromString(stringValue.getValue()));
			}
		}
		return objectValues;

	}
}
