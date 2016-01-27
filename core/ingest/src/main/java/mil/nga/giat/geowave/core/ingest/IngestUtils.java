package mil.nga.giat.geowave.core.ingest;

import java.io.IOException;
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

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.index.IndexOptionProviderSpi;
import mil.nga.giat.geowave.core.ingest.index.IndexProvider;
import mil.nga.giat.geowave.core.ingest.index.IngestDimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.core.ingest.local.IngestRunData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

public class IngestUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestUtils.class);
	private static Map<String, IngestDimensionalityTypeProviderSpi> registeredDimensionalityTypes = null;
	private static SortedSet<IndexOptionProviderSpi> registeredIndexOptions = null;
	private static String defaultDimensionalityType;

	public static <T> void ingest(
			final T input,
			final IngestCommandLineOptions ingestOptions,
			final IngestPluginBase<T, ?> ingestPlugin,
			final IndexProvider indexProvider,
			final IngestRunData ingestRunData )
			throws IOException {
		final String[] dimensionTypes = ingestOptions.getDimensionalityTypes();
		final Map<ByteArrayId, IndexWriter> dimensionalityIndexMap = new HashMap<ByteArrayId, IndexWriter>();
		for (final String dimensionType : dimensionTypes) {
			final PrimaryIndex primaryIndex = getIndex(
					ingestPlugin,
					ingestRunData.getArgs(),
					dimensionType);
			if (primaryIndex == null) {
				LOGGER.error("Could not get index instance, getIndex() returned null;");
				throw new IOException(
						"Could not get index instance, getIndex() returned null");
			}
			final IndexWriter primaryIndexWriter = ingestRunData.getIndexWriter(primaryIndex);
			final PrimaryIndex idx = primaryIndexWriter.getIndex();
			if (idx == null) {
				LOGGER.error("Could not get index instance, getIndex() returned null;");
				throw new IOException(
						"Could not get index instance, getIndex() returned null");
			}
			dimensionalityIndexMap.put(
					idx.getId(),
					primaryIndexWriter);
		}

		final Map<ByteArrayId, PrimaryIndex> requiredIndexMap = new HashMap<ByteArrayId, PrimaryIndex>();
		final PrimaryIndex[] requiredIndices = indexProvider.getRequiredIndices();
		if ((requiredIndices != null) && (requiredIndices.length > 0)) {
			for (final PrimaryIndex requiredIndex : requiredIndices) {
				requiredIndexMap.put(
						requiredIndex.getId(),
						requiredIndex);
			}
		}
		try (CloseableIterator<?> geowaveDataIt = ingestPlugin.toGeoWaveData(
				input,
				dimensionalityIndexMap.keySet(),
				ingestOptions.getVisibility())) {
			while (geowaveDataIt.hasNext()) {
				final GeoWaveData<?> geowaveData = (GeoWaveData<?>) geowaveDataIt.next();
				final WritableDataAdapter adapter = ingestRunData.getDataAdapter(geowaveData);
				if (adapter == null) {
					LOGGER.warn("Adapter not found for " + geowaveData.getValue());
					continue;
				}
				IndexWriter indexWriter;
				for (final ByteArrayId indexId : geowaveData.getIndexIds()) {
					indexWriter = dimensionalityIndexMap.get(indexId);
					if (indexWriter == null) {
						final PrimaryIndex index = requiredIndexMap.get(indexId);
						if (index == null) {
							LOGGER.warn("Index '" + indexId.getString() + "' not found for " + geowaveData.getValue());
							continue;
						}
						indexWriter = ingestRunData.getIndexWriter(index);
					}
					indexWriter.write(
							adapter,
							geowaveData.getValue());
				}
			}
		}
	}

	public static PrimaryIndex getIndex(
			final DataAdapterProvider<?> adapterProvider,
			final String[] args,
			final String dimensionalityTypeStr ) {
		final IngestDimensionalityTypeProviderSpi dimensionalityType = getSelectedDimensionalityProvider(dimensionalityTypeStr);

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

	public static boolean isSupported(
			final DataAdapterProvider<?> adapterProvider,
			final String[] args,
			final String[] dimensionalityTypes ) {
		for (final String dimensionalityType : dimensionalityTypes) {
			if (isSupported(
					adapterProvider,
					args,
					dimensionalityType)) {
				return true;
			}
		}
		return false;

	}

	public static boolean isSupported(
			final DataAdapterProvider<?> adapterProvider,
			final String[] args,
			final String dimensionalityTypeStr ) {
		return (getIndex(
				adapterProvider,
				args,
				dimensionalityTypeStr) != null);
	}

	protected static String getDefaultDimensionalityType() {
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

	protected static synchronized String getDimensionalityTypeOptionDescription() {
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
