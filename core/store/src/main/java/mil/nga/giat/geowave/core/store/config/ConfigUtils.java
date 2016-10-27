package mil.nga.giat.geowave.core.store.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPropertiesTransformer;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;
import mil.nga.giat.geowave.core.cli.prefix.TranslationEntry;

public class ConfigUtils
{
	public static String cleanOptionName(
			String name ) {
		name = name.trim().toLowerCase(
				Locale.ENGLISH).replaceAll(
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
			final List<ConfigOption> options = new ArrayList<ConfigOption>();
			for (TranslationEntry entry : entries) {
				if (!entry.isHidden()) {
					ConfigOption opt = new ConfigOption(
							entry.getAsPropertyName(),
							entry.getDescription(),
							!entry.isRequired(),
							entry.getParam().getType());
					opt.setPassword(entry.isPassword());
					options.add(opt);
				}
			}
			opts = options.toArray(new ConfigOption[options.size()]);
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
