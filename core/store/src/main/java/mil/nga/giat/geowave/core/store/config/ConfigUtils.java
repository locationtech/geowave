package mil.nga.giat.geowave.core.store.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPropertiesTransformer;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;
import mil.nga.giat.geowave.core.cli.prefix.TranslationEntry;

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

	/**
	 * This method will use the parameter descriptions from JCommander to
	 * create/populate an AbstractConfigOptions map.
	 */
	public static ConfigOption[] createConfigOptionsFromJCommander(
			Object createOptionsInstance ) {
		ConfigOption[] opts = null;
		if (createOptionsInstance != null) {
			JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
			translator.addObject(createOptionsInstance);
			JCommanderTranslationMap map = translator.translate();
			Collection<TranslationEntry> entries = map.getEntries().values();
			opts = new ConfigOption[entries.size()];
			int optCounter = 0;
			for (TranslationEntry entry : entries) {
				ConfigOption opt = new ConfigOption(
						entry.getAsPropertyName(),
						entry.getDescription(),
						!entry.isRequired());
				opt.setPassword(entry.isPassword());
				opts[optCounter++] = opt;
			}
		}
		else {
			opts = new ConfigOption[0];
		}
		return opts;
	}

	/**
	 * Take the given options and populate the given options list. This is
	 * JCommander specific.
	 */
	public static <T> T populateOptionsFromList(
			T optionsObject,
			Map<String, String> optionList ) {
		if (optionsObject != null) {
			JCommanderPropertiesTransformer translator = new JCommanderPropertiesTransformer();
			translator.addObject(optionsObject);
			translator.transformFromMap(optionList);
		}
		return optionsObject;
	}

	/**
	 * Take the given options and populate the given options list. This is
	 * JCommander specific.
	 */
	public static Map<String, String> populateListFromOptions(
			Object optionsObject ) {
		Map<String, String> mapOptions = new HashMap<String, String>();
		if (optionsObject != null) {
			JCommanderPropertiesTransformer translator = new JCommanderPropertiesTransformer();
			translator.addObject(optionsObject);
			translator.transformToMap(mapOptions);
		}
		return mapOptions;
	}
}
