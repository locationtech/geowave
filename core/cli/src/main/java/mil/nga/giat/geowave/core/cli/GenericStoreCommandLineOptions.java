package mil.nga.giat.geowave.core.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.cli.CommandLineOptions.CommandLineWrapper;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.filter.GenericTypeResolver;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

abstract public class GenericStoreCommandLineOptions<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GenericStoreCommandLineOptions.class);
	protected final GenericStoreFactory<T> factory;
	protected final Map<String, Object> configOptions;
	protected final String namespace;
	public static String NAMESPACE_OPTION_KEY = "gwNamespace";
	public static String CONNECTION_OPTION_KEY = "connectionParams";

	public GenericStoreCommandLineOptions(
			final GenericStoreFactory<T> factory,
			final Map<String, Object> configOptions,
			final String namespace ) {
		this.factory = factory;
		this.configOptions = configOptions;
		this.namespace = namespace;
	}

	public T createStore() {
		if (factory != null) {
			return factory.createStore(
					configOptions,
					namespace);
		}
		return null;
	}

	public GenericStoreFactory<T> getFactory() {
		return factory;
	}

	public Map<String, Object> getConfigOptions() {
		return configOptions;
	}

	public String getNamespace() {
		return namespace;
	}

	private static Options storeOptionsToCliOptions(
			final String prefix,
			final AbstractConfigOption<?>[] storeOptions ) {
		final Options cliOptions = new Options();
		for (final AbstractConfigOption<?> storeOption : storeOptions) {
			cliOptions.addOption(storeOptionToCliOption(
					prefix,
					storeOption));
		}
		return cliOptions;
	}

	protected static Option storeOptionToCliOption(
			final String prefix,
			final AbstractConfigOption<?> storeOption ) {
		final Class<?> cls = GenericTypeResolver.resolveTypeArgument(
				storeOption.getClass(),
				AbstractConfigOption.class);
		final boolean isBoolean = Boolean.class.isAssignableFrom(cls);
		final Option cliOption = new Option(
				getCleanedOptionName(
						prefix,
						storeOption.getName()),
				!isBoolean,
				storeOption.getDescription());
		cliOption.setRequired(!storeOption.isOptional() && !isBoolean);
		return cliOption;
	}

	private static String getCleanedOptionName(
			final String prefix,
			final String originalOptionName ) {
		return ConfigUtils.cleanOptionName(prefix != null ? prefix + originalOptionName : originalOptionName);
	}

	private static String[] getArgs(
			final String[] existingArgs,
			final String prefix,
			final Map<String, String> connectionParams ) {
		if ((connectionParams == null) || connectionParams.isEmpty()) {
			return existingArgs;
		}
		final List<String> argsWithConnectionParams = new ArrayList<String>(
				Arrays.asList(existingArgs));
		for (final Entry<String, String> connectionParam : connectionParams.entrySet()) {
			final String key = getCleanedOptionName(
					prefix,
					connectionParam.getKey());
			if (connectionParam.getValue().equalsIgnoreCase(
					"true")) {
				argsWithConnectionParams.add("-" + key);
			}
			else if (connectionParam.getValue().equalsIgnoreCase(
					"false")) {
				continue;
			}
			else {
				argsWithConnectionParams.add("-" + key);
				argsWithConnectionParams.add(connectionParam.getValue());
			}
		}
		return argsWithConnectionParams.toArray(new String[] {});
	}

	public static Pair<Map<String, Object>, CommandLine> getConfigOptionsForStoreFactory(
			final String prefix,
			final Options currentOptions,
			final Map<String, String> connectionParams,
			final CommandLineOptions currentCommandLine,
			final GenericStoreFactory<?> genericStoreFactory )
			throws Exception {
		final AbstractConfigOption<?>[] storeOptions = genericStoreFactory.getOptions();
		final Options cliOptions = storeOptionsToCliOptions(
				prefix,
				storeOptions);
		if (currentOptions != null) {
			final Collection<Option> options = currentOptions.getOptions();
			for (final Option o : options) {
				final Option opt = (Option) o.clone();
				opt.setRequired(false);
				cliOptions.addOption(opt);
			}
		}
		final BasicParser parser = new BasicParser();

		final String[] args = getArgs(
				currentCommandLine.getArgs(),
				prefix,
				connectionParams);
		// parse the datastore options
		final CommandLine commandLineWithStoreOptions = parser.parse(
				cliOptions,
				args,
				true);

		final Map<String, Object> configOptions = new HashMap<String, Object>();
		for (final AbstractConfigOption<?> option : storeOptions) {
			final String cliOptionName = ConfigUtils.cleanOptionName(prefix != null ? prefix + option.getName() : option.getName());
			final Class<?> cls = GenericTypeResolver.resolveTypeArgument(
					option.getClass(),
					AbstractConfigOption.class);
			final boolean isBoolean = Boolean.class.isAssignableFrom(cls);
			final boolean hasOption = commandLineWithStoreOptions.hasOption(cliOptionName);
			if (isBoolean) {
				configOptions.put(
						option.getName(),
						option.valueFromString(hasOption ? "true" : "false"));
			}
			else if (hasOption) {
				final String optionValueStr = commandLineWithStoreOptions.getOptionValue(cliOptionName);
				configOptions.put(
						option.getName(),
						option.valueFromString(optionValueStr));
			}
			else if (connectionParams.containsKey(option.getName())) {
				configOptions.put(
						option.getName(),
						option.valueFromString(connectionParams.get(option.getName())));
			}
		}
		final Option[] newOptions = commandLineWithStoreOptions.getOptions();
		final Option[] prevOptions = currentCommandLine.getOptions();

		final String[] newArgs = commandLineWithStoreOptions.getArgs();
		final String[] prevArgs = currentCommandLine.getArgs();
		final boolean unchanged = Arrays.equals(
				newArgs,
				prevArgs) && Arrays.equals(
				newOptions,
				prevOptions);
		return new ImmutablePair<Map<String, Object>, CommandLine>(
				configOptions,
				unchanged ? null : commandLineWithStoreOptions);
	}

	public static <T, F extends GenericStoreFactory<T>> void applyOptions(
			final String prefix,
			final Options allOptions,
			final CommandLineHelper<T, F> helper ) {
		final String optionName = helper.getOptionName();
		allOptions.addOption(new Option(
				prefix != null ? prefix + optionName : optionName,
				true,
				"Explicitly set the " + optionName + " by name, if not set, an " + optionName + " will be used if all of its required options are provided. " + ConfigUtils.getOptions(
						helper.getRegisteredFactories().keySet(),
						"Available " + optionName + "s: ")));
		final Option namespace = new Option(
				prefix != null ? prefix + NAMESPACE_OPTION_KEY : NAMESPACE_OPTION_KEY,
				true,
				"The geowave namespace (optional; default is no namespace)");
		namespace.setRequired(false);
		allOptions.addOption(namespace);
		final Option connectionString = new Option(
				prefix != null ? prefix + CONNECTION_OPTION_KEY : CONNECTION_OPTION_KEY,
				true,
				"A semicolon delimited set of <key>=<value> pairs of connection parameters that fully describes how to connect to the desired " + optionName + ". For example to connect to accumulo an example would be 'zookeeper=zookeeperhost:2181;user=accumulouser;password=mypassword;instance=accumuloinstance'");
		connectionString.setRequired(false);
		allOptions.addOption(connectionString);
	}

	public static <T, F extends GenericStoreFactory<T>> CommandLineResult<GenericStoreCommandLineOptions<T>> parseOptions(
			final String prefix,
			final Options options,
			final CommandLine commandLine,
			final CommandLineHelper<T, F> helper )
			throws ParseException {
		return parseOptions(
				prefix,
				options,
				new CommandLineWrapper(
						commandLine),
				helper);
	}

	protected static <T, F extends GenericStoreFactory<T>> F getSelectedStore(
			final String optionName,
			final CommandLineOptions commandLine,
			final CommandLineHelper<T, F> helper )
			throws ParseException {
		if (commandLine.hasOption(optionName)) {
			// if data store is given, make sure the commandline options
			// properly match the options for this store
			final String selectedStoreName = commandLine.getOptionValue(optionName);
			final F selectedStoreFactory = helper.getRegisteredFactories().get(
					selectedStoreName);
			if (selectedStoreFactory == null) {
				final String errorMsg = "Cannot find selected " + optionName + " '" + selectedStoreName + "'";
				LOGGER.error(errorMsg);
				throw new ParseException(
						errorMsg);
			}
			return selectedStoreFactory;
		}
		return null;
	}

	public static void applyStoreOptions(
			final GenericStoreFactory<?> storeFactory,
			final Options options )
			throws ParseException {
		final List<Option> optionsList = Lists.transform(
				Lists.newArrayList(storeFactory.getOptions()),
				new GeoWaveStoreOptionToCliOption());
		for (final Option o : optionsList) {
			options.addOption(o);
		}
	}

	public static <T, F extends GenericStoreFactory<T>> CommandLineResult<GenericStoreCommandLineOptions<T>> parseOptions(
			final String prefix,
			final Options options,
			final CommandLineOptions commandLine,
			final CommandLineHelper<T, F> helper )
			throws ParseException {
		final String optionName = prefix != null ? prefix + helper.getOptionName() : helper.getOptionName();
		final String namespace = commandLine.getOptionValue(
				prefix != null ? prefix + NAMESPACE_OPTION_KEY : NAMESPACE_OPTION_KEY,
				"");
		final String connectionParams = commandLine.getOptionValue(
				prefix != null ? prefix + CONNECTION_OPTION_KEY : CONNECTION_OPTION_KEY,
				"");
		Map<String, String> connectionParamsMap;
		if ((connectionParams != null) && !connectionParams.isEmpty()) {
			try {
				connectionParamsMap = StringUtils.parseParams(connectionParams);
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to correctly parse connection params '" + connectionParams + "': Ignoring " + CONNECTION_OPTION_KEY,
						e);
				connectionParamsMap = new HashMap<String, String>();
			}
		}
		else {
			connectionParamsMap = new HashMap<String, String>();
		}
		if (commandLine.hasOption(optionName)) {
			// if data store is given, make sure the commandline options
			// properly match the options for this store
			final String selectedStoreName = commandLine.getOptionValue(optionName);
			final F selectedStoreFactory = helper.getRegisteredFactories().get(
					selectedStoreName);
			if (selectedStoreFactory == null) {
				final String errorMsg = "Cannot find selected " + optionName + " '" + selectedStoreName + "'";
				LOGGER.error(errorMsg);
				throw new ParseException(
						errorMsg);
			}

			try {

				// if (connectionParamsMap == null) {
				final Pair<Map<String, Object>, CommandLine> configOptionsCmdLinePair = getConfigOptionsForStoreFactory(
						prefix,
						options,
						connectionParamsMap,
						commandLine,
						selectedStoreFactory);
				return new CommandLineResult<GenericStoreCommandLineOptions<T>>(
						helper.createCommandLineOptions(
								selectedStoreFactory,
								configOptionsCmdLinePair.getLeft(),
								namespace),
						configOptionsCmdLinePair.getRight() != null,
						configOptionsCmdLinePair.getRight());
				// }
				// else {
				// return new
				// CommandLineResult<GenericStoreCommandLineOptions<T>>(
				// helper.createCommandLineOptions(
				// selectedStoreFactory,
				// configOptions,
				// namespace),
				// false,
				// null);
				// }
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to parse config options for " + optionName + " '" + selectedStoreName + "'",
						e);
				throw new ParseException(
						"Unable to parse config options for  " + optionName + " '" + selectedStoreName + "'; " + e.getMessage());
			}
		}
		// if data store is not given, go through all available data stores
		// until one matches the config options
		final Map<String, F> factories = helper.getRegisteredFactories();
		final Map<String, Exception> exceptionsPerDataStoreFactory = new HashMap<String, Exception>();
		int matchingCommandLineOptionCount = -1;
		CommandLineResult<GenericStoreCommandLineOptions<T>> matchingCommandLineOptions = null;
		boolean matchingCommandLineOptionsHaveSameOptionCount = false;
		// if the hint is not provided, the parser will attempt to find
		// a factory that does not have any missing options; if multiple
		// factories will match, the one with the most options will be used with
		// the assumption that it has the most specificity and closest match of
		// the arguments; if there are multiple factories that match and have
		// the same number of options, arbitrarily the last one will be chosen
		// and a warning message will be logged

		for (final Entry<String, F> factoryEntry : factories.entrySet()) {
			try {
				final Pair<Map<String, Object>, CommandLine> configOptionsCmdLinePair = getConfigOptionsForStoreFactory(
						prefix,
						options,
						connectionParamsMap,
						commandLine,
						factoryEntry.getValue());

				final GenericStoreCommandLineOptions<T> commandLineOptions = helper.createCommandLineOptions(
						factoryEntry.getValue(),
						configOptionsCmdLinePair.getLeft(),
						namespace);
				if (commandLineOptions.getFactory().getOptions().length >= matchingCommandLineOptionCount) {
					matchingCommandLineOptions = new CommandLineResult<GenericStoreCommandLineOptions<T>>(
							commandLineOptions,
							configOptionsCmdLinePair.getRight() != null,
							configOptionsCmdLinePair.getRight());
					matchingCommandLineOptionsHaveSameOptionCount = (commandLineOptions.getFactory().getOptions().length == matchingCommandLineOptionCount);
					matchingCommandLineOptionCount = commandLineOptions.getFactory().getOptions().length;
				}
			}
			catch (final Exception e) {
				// it just means this store is not compatible with the
				// options, add it to a list and we'll log it only if no store
				// is compatible
				exceptionsPerDataStoreFactory.put(
						factoryEntry.getKey(),
						e);
			}
		}
		if (matchingCommandLineOptions == null) {
			// just log all the exceptions so that it is apparent where the
			// commandline incompatibility might be
			for (final Entry<String, Exception> exceptionEntry : exceptionsPerDataStoreFactory.entrySet()) {
				LOGGER.error(
						"Could not parse commandline for " + optionName + " '" + exceptionEntry.getKey() + "'",
						exceptionEntry.getValue());
			}
			throw new ParseException(
					"No compatible " + optionName + " found");
		}
		else if (matchingCommandLineOptionsHaveSameOptionCount) {
			LOGGER.warn("Multiple valid stores found with equal specificity for " + helper.getOptionName() + " store");
			LOGGER.warn(matchingCommandLineOptions.getResult().getFactory().getName() + " will be automatically chosen");
		}
		return matchingCommandLineOptions;
	}

	public static CommandLine getCommandLineFromConfigOptions(
			final Map<String, String> configOptions,
			final GenericStoreFactory<?> genericStoreFactory )
			throws Exception {
		final AbstractConfigOption<?>[] storeOptions = genericStoreFactory.getOptions();

		final List<String> args = new ArrayList<String>();
		for (final AbstractConfigOption<?> option : storeOptions) {
			final String cliOptionName = ConfigUtils.cleanOptionName(option.getName());
			final Class<?> cls = GenericTypeResolver.resolveTypeArgument(
					option.getClass(),
					AbstractConfigOption.class);
			final boolean isBoolean = Boolean.class.isAssignableFrom(cls);
			final String value = configOptions.get(option.getName());
			if (value != null) {
				if (isBoolean) {
					if (value.equalsIgnoreCase("true")) {
						args.add("-" + cliOptionName);
					}
				}
				else {
					args.add("-" + cliOptionName);
					args.add(value);
				}
			}
		}
		final BasicParser parser = new BasicParser();
		final Options options = new Options();
		GenericStoreCommandLineOptions.applyStoreOptions(
				genericStoreFactory,
				options);
		return parser.parse(
				options,
				args.toArray(new String[] {}),
				true);
	}

	protected static interface CommandLineHelper<T, F extends GenericStoreFactory<T>>
	{
		public Map<String, F> getRegisteredFactories();

		public String getOptionName();

		public GenericStoreCommandLineOptions<T> createCommandLineOptions(
				GenericStoreFactory<T> factory,
				Map<String, Object> configOptions,
				String namespace );
	}

	private static class GeoWaveStoreOptionToCliOption implements
			Function<AbstractConfigOption<?>, Option>
	{
		@Override
		public Option apply(
				final AbstractConfigOption<?> input ) {
			return GenericStoreCommandLineOptions.storeOptionToCliOption(
					null,
					input);
		}
	}
}
