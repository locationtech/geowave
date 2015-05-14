package mil.nga.giat.geowave.core.ingest;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestCommandLineOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestCommandLineOptions.class);

	private static Map<String, IndexCompatibilityVisitor> registeredDimensionalityTypes = null;
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

	public Index getIndex(
			final Index[] supportedIndices ) {
		final IndexCompatibilityVisitor compatibilityVisitor = getSelectedIndexCompatibility(getDimensionalityType());
		for (final Index i : supportedIndices) {
			if (compatibilityVisitor.isCompatible(i)) {
				return i;
			}
		}
		return null;
	}

	public boolean isSupported(
			final Index[] supportedIndices ) {
		return (getIndex(supportedIndices) != null);
	}

	private static synchronized String getDimensionalityTypeOptionDescription() {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}
		if (registeredDimensionalityTypes.isEmpty()) {
			return "There are no registered dimensionality types.  The supported index listed first for any given data type will be used.";
		}
		final StringBuilder builder = new StringBuilder();
		for (final String dimType : registeredDimensionalityTypes.keySet()) {
			if (builder.length() > 0) {
				builder.append(",");
			}
			else {
				builder.append("Options include: ");
			}
			builder.append(
					"'").append(
					dimType).append(
					"'");
		}
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

	private static IndexCompatibilityVisitor getSelectedIndexCompatibility(
			final String dimensionalityType ) {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}
		final IndexCompatibilityVisitor compatibilityVisitor = registeredDimensionalityTypes.get(dimensionalityType);
		if (compatibilityVisitor == null) {
			return new IndexCompatibilityVisitor() {

				@Override
				public boolean isCompatible(
						final Index index ) {
					return true;
				}
			};
		}
		return compatibilityVisitor;
	}

	private static synchronized void initDimensionalityTypeRegistry() {
		registeredDimensionalityTypes = new HashMap<String, IndexCompatibilityVisitor>();
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
						dimensionalityTypeProvider.getCompatibilityVisitor());
				if (dimensionalityTypeProvider.getPriority() > currentDefaultPriority) {
					currentDefaultPriority = dimensionalityTypeProvider.getPriority();
					defaultDimensionalityType = dimensionalityTypeProvider.getDimensionalityTypeName();
				}
			}
		}
	}

	public static IngestCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final boolean success = true;
		boolean clearNamespace = false;
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
}
