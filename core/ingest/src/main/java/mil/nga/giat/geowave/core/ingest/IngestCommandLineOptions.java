package mil.nga.giat.geowave.core.ingest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.SortedSet;
import java.util.TreeSet;

import mil.nga.giat.geowave.core.ingest.index.IndexOptionProviderSpi;
import mil.nga.giat.geowave.core.ingest.index.IngestDimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

public class IngestCommandLineOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestCommandLineOptions.class);

	private static Map<String, IngestDimensionalityTypeProviderSpi> registeredDimensionalityTypes = null;
	private static SortedSet<IndexOptionProviderSpi> registeredIndexOptions = null;
	private static String defaultDimensionalityType;
	private final String visibility;
	private final boolean clearNamespace;
	private final String dimensionalityType;

	public IngestCommandLineOptions(
			final String visibility,
			final boolean clearNamespace,
			final String dimensionalityType ) {
		this.visibility = visibility;
		this.clearNamespace = clearNamespace;
		this.dimensionalityType = dimensionalityType;
	}

	public String getVisibility() {
		return visibility;
	}

	public String getDimensionalityType() {
		return dimensionalityType;
	}

	public boolean isClearNamespace() {
		return clearNamespace;
	}

	public PrimaryIndex getIndex(
			final DataAdapterProvider<?> adapterProvider,
			final String[] args ) {
		final IngestDimensionalityTypeProviderSpi dimensionalityType = getSelectedDimensionalityProvider(getDimensionalityType());

		if (isCompatible(
				adapterProvider,
				dimensionalityType)) {
			final JCommander commander = new JCommander();
			commander.addObject(dimensionalityType.getOptions());

			final List<Object> options = getIndexOptions();
			for (final Object opt : options) {
				commander.addObject(opt);
			}
			commander.setAcceptUnknownOptions(true);
			commander.parse(args);
			final PrimaryIndex index = dimensionalityType.createPrimaryIndex();
			return wrapIndexWithOptions(index);

		}
		return null;
	}

	public boolean isSupported(
			final DataAdapterProvider<?> adapterProvider,
			final String[] args ) {
		return (getIndex(
				adapterProvider,
				args) != null);
	}

	private static synchronized String getDimensionalityTypeOptionDescription() {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}
		if (registeredDimensionalityTypes.isEmpty()) {
			return "There are no registered dimensionality types.  The supported index listed first for any given data type will be used.";
		}
		final StringBuilder builder = ConfigUtils.getOptions(registeredDimensionalityTypes.keySet());
		builder.append(
				"(optional; default is '").append(
				defaultDimensionalityType).append(
				"')");
		return builder.toString();
	}

	private static String getDefaultDimensionalityType() {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}
		if (defaultDimensionalityType == null) {
			return "";
		}
		return defaultDimensionalityType;
	}

	private static synchronized List<Object> getIndexOptions() {
		if (registeredIndexOptions == null) {
			initIndexOptionRegistry();
		}
		final List<Object> options = new ArrayList<Object>();
		for (final IndexOptionProviderSpi optionProvider : registeredIndexOptions) {
			final Object optionsObj = optionProvider.getOptions();
			if (optionsObj != null) {
				options.add(optionsObj);
			}
		}
		return options;
	}

	private static PrimaryIndex wrapIndexWithOptions(
			final PrimaryIndex index ) {
		PrimaryIndex retVal = index;
		for (final IndexOptionProviderSpi optionProvider : registeredIndexOptions) {
			retVal = optionProvider.wrapIndexWithOptions(retVal);
		}
		return retVal;
	}

	private static IngestDimensionalityTypeProviderSpi getSelectedDimensionalityProvider(
			final String dimensionalityType ) {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}

		return registeredDimensionalityTypes.get(dimensionalityType);
	}

	/**
	 * Determine whether an index is compatible with the visitor
	 * 
	 * @param index
	 *            an index that an ingest type supports
	 * @return whether the adapter is compatible with the common index model
	 */
	public static boolean isCompatible(
			final DataAdapterProvider<?> adapterProvider,
			final IngestDimensionalityTypeProviderSpi dimensionalityProvider ) {
		final Class<? extends CommonIndexValue>[] supportedTypes = adapterProvider.getSupportedIndexableTypes();
		if ((supportedTypes == null) || (supportedTypes.length == 0)) {
			return false;
		}
		final Class<? extends CommonIndexValue>[] requiredTypes = dimensionalityProvider.getRequiredIndexTypes();
		for (final Class<? extends CommonIndexValue> requiredType : requiredTypes) {
			boolean fieldFound = false;
			for (final Class<? extends CommonIndexValue> supportedType : supportedTypes) {
				if (requiredType.isAssignableFrom(supportedType)) {
					fieldFound = true;
					break;
				}
			}
			if (!fieldFound) {
				return false;
			}
		}
		return true;

	}

	private static synchronized void initDimensionalityTypeRegistry() {
		registeredDimensionalityTypes = new HashMap<String, IngestDimensionalityTypeProviderSpi>();
		final Iterator<IngestDimensionalityTypeProviderSpi> dimensionalityTypesProviders = ServiceLoader.load(
				IngestDimensionalityTypeProviderSpi.class).iterator();
		int currentDefaultPriority = Integer.MIN_VALUE;
		while (dimensionalityTypesProviders.hasNext()) {
			final IngestDimensionalityTypeProviderSpi dimensionalityTypeProvider = dimensionalityTypesProviders.next();
			if (registeredDimensionalityTypes.containsKey(dimensionalityTypeProvider.getDimensionalityTypeName())) {
				LOGGER.warn("Dimensionality type '" + dimensionalityTypeProvider.getDimensionalityTypeName() + "' already registered.  Unable to register type provided by " + dimensionalityTypeProvider.getClass().getName());
			}
			else {
				registeredDimensionalityTypes.put(
						dimensionalityTypeProvider.getDimensionalityTypeName(),
						dimensionalityTypeProvider);
				if (dimensionalityTypeProvider.getPriority() > currentDefaultPriority) {
					currentDefaultPriority = dimensionalityTypeProvider.getPriority();
					defaultDimensionalityType = dimensionalityTypeProvider.getDimensionalityTypeName();
				}
			}
		}
	}

	private static synchronized void initIndexOptionRegistry() {
		registeredIndexOptions = new TreeSet<IndexOptionProviderSpi>(
				new IndexOptionComparator());
		final Iterator<IndexOptionProviderSpi> indexOptionProviders = ServiceLoader.load(
				IndexOptionProviderSpi.class).iterator();
		while (indexOptionProviders.hasNext()) {
			registeredIndexOptions.add(indexOptionProviders.next());
		}
	}

	public static IngestCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final boolean success = true;
		boolean clearNamespace = false;
		final int randomPartitions = -1;
		if (commandLine.hasOption("c")) {
			clearNamespace = true;
		}
		String visibility = null;
		if (commandLine.hasOption("v")) {
			visibility = commandLine.getOptionValue("v");
		}
		final String dimensionalityType = commandLine.getOptionValue(
				"dim",
				getDefaultDimensionalityType());
		if (!success) {
			throw new ParseException(
					"Required option is missing");
		}
		return new IngestCommandLineOptions(
				visibility,
				clearNamespace,
				dimensionalityType);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option visibility = new Option(
				"v",
				"visibility",
				true,
				"The visibility of the data ingested (optional; default is 'public')");
		allOptions.addOption(visibility);

		final Option dimensionalityType = new Option(
				"dim",
				"dimensionality",
				true,
				"The preferred dimensionality type to index the data for this ingest operation. " + getDimensionalityTypeOptionDescription());
		allOptions.addOption(dimensionalityType);
		allOptions.addOption(new Option(
				"c",
				"clear",
				false,
				"Clear ALL data stored with the same prefix as this namespace (optional; default is to append data to the namespace if it exists)"));
	}

	private static class IndexOptionComparator implements
			Comparator<IndexOptionProviderSpi>,
			Serializable
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(
				final IndexOptionProviderSpi arg0,
				final IndexOptionProviderSpi arg1 ) {
			return Integer.compare(
					arg0.getResolutionOrder(),
					arg1.getResolutionOrder());
		}

	}
}
