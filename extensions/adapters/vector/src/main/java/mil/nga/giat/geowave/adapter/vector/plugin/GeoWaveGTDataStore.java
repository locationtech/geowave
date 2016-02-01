package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.auth.AuthorizationSPI;
import mil.nga.giat.geowave.adapter.vector.index.IndexQueryStrategySPI;
import mil.nga.giat.geowave.adapter.vector.index.SimpleFeaturePrimaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.plugin.lock.LockingManagement;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveAutoCommitTransactionState;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransactionManagementState;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransactionState;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.MemoryTransactionsAllocator;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.ColumnVisibilityManagement;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.VisibilityManagementHelper;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.AdapterIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.apache.log4j.Logger;
import org.geotools.data.FeatureListenerManager;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class GeoWaveGTDataStore extends
		ContentDataStore
{
	/** Package logger */
	private final static Logger LOGGER = Logger.getLogger(GeoWaveGTDataStore.class);
	public static final CoordinateReferenceSystem DEFAULT_CRS;

	static {
		try {
			DEFAULT_CRS = CRS.decode(
					"EPSG:4326",
					true);
		}
		catch (final FactoryException e) {
			LOGGER.error(
					"Unable to decode EPSG:4326 CRS",
					e);
			throw new RuntimeException(
					"Unable to initialize EPSG:4326 object",
					e);
		}
	}

	private FeatureListenerManager listenerManager = null;
	protected AdapterStore adapterStore;
	protected IndexStore indexStore;
	protected DataStatisticsStore dataStatisticsStore;
	protected DataStore dataStore;
	private final Map<String, List<PrimaryIndex>> preferredIndexes = new ConcurrentHashMap<String, List<PrimaryIndex>>();

	private final ColumnVisibilityManagement<SimpleFeature> visibilityManagement = VisibilityManagementHelper.loadVisibilityManagement();
	private final AuthorizationSPI authorizationSPI;
	private final IndexQueryStrategySPI indexQueryStrategy;
	private final URI featureNameSpaceURI;
	private int transactionBufferSize = 10000;
	private final TransactionsAllocator transactionsAllocator;

	public GeoWaveGTDataStore(
			final GeoWavePluginConfig config )
			throws IOException {
		listenerManager = new FeatureListenerManager();
		lockingManager = config.getLockingManagementFactory().createLockingManager(
				config);
		authorizationSPI = config.getAuthorizationFactory().create(
				config.getAuthorizationURL());
		init(config);
		featureNameSpaceURI = config.getFeatureNamespace();
		indexQueryStrategy = config.getIndexQueryStrategy();
		transactionBufferSize = config.getTransactionBufferSize();
		transactionsAllocator = new MemoryTransactionsAllocator();

	}

	public void init(
			final GeoWavePluginConfig config ) {
		dataStore = config.getDataStore();
		dataStatisticsStore = config.getDataStatisticsStore();
		indexStore = config.getIndexStore();
		adapterStore = config.getAdapterStore();
	}

	public AuthorizationSPI getAuthorizationSPI() {
		return authorizationSPI;
	}

	public FeatureListenerManager getListenerManager() {
		return listenerManager;
	}

	public IndexQueryStrategySPI getIndexQueryStrategy() {
		return indexQueryStrategy;
	}

	public DataStore getDataStore() {
		return dataStore;
	}

	public AdapterStore getAdapterStore() {
		return adapterStore;
	}

	public IndexStore getIndexStore() {
		return indexStore;
	}

	public DataStatisticsStore getDataStatisticsStore() {
		return dataStatisticsStore;
	}

	protected List<PrimaryIndex> getWriteIndices(
			final GeotoolsFeatureDataAdapter adapter ) {
		if (adapter instanceof FeatureDataAdapter) {
			return getPreferredIndex((FeatureDataAdapter) adapter);
		}
		return Arrays.asList(new SpatialDimensionalityTypeProvider().createPrimaryIndex());
	}

	@Override
	public void createSchema(
			final SimpleFeatureType featureType ) {
		if (featureType.getGeometryDescriptor() == null) {
			throw new UnsupportedOperationException(
					"Schema missing geometry");
		}
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				featureType,
				visibilityManagement);
		if (featureNameSpaceURI != null) {
			adapter.setNamespace(featureNameSpaceURI.toString());
		}

		adapterStore.addAdapter(adapter);
		getPreferredIndex(adapter);
	}

	private GeotoolsFeatureDataAdapter getAdapter(
			final String typeName ) {
		final GeotoolsFeatureDataAdapter featureAdapter;
		final DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
				StringUtils.stringToBinary(typeName)));
		if ((adapter == null) || !(adapter instanceof GeotoolsFeatureDataAdapter)) {
			return null;
		}
		featureAdapter = (GeotoolsFeatureDataAdapter) adapter;
		if (featureNameSpaceURI != null) {
			if (adapter instanceof FeatureDataAdapter) {
				((FeatureDataAdapter) featureAdapter).setNamespace(featureNameSpaceURI.toString());
			}
		}
		return featureAdapter;
	}

	@Override
	protected List<Name> createTypeNames()
			throws IOException {
		final List<Name> names = new ArrayList<>();
		final CloseableIterator<DataAdapter<?>> adapters = adapterStore.getAdapters();
		while (adapters.hasNext()) {
			final DataAdapter<?> adapter = adapters.next();
			if (adapter instanceof GeotoolsFeatureDataAdapter) {
				names.add(new NameImpl(
						((GeotoolsFeatureDataAdapter) adapter).getType().getTypeName()));
			}
		}
		adapters.close();
		return names;
	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final String typeName )
			throws IOException {
		return getFeatureSource(
				typeName,
				Transaction.AUTO_COMMIT);
	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final String typeName,
			final Transaction tx )
			throws IOException {
		return super.getFeatureSource(
				new NameImpl(
						null,
						typeName),
				tx);
	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final Name typeName,
			final Transaction tx )
			throws IOException {
		return getFeatureSource(
				typeName.getLocalPart(),
				tx);

	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final Name typeName )
			throws IOException {
		return getFeatureSource(
				typeName.getLocalPart(),
				Transaction.AUTO_COMMIT);
	}

	@Override
	protected ContentFeatureSource createFeatureSource(
			final ContentEntry entry )
			throws IOException {
		return new GeoWaveFeatureSource(
				entry,
				Query.ALL,
				getAdapter(entry.getTypeName()),
				transactionsAllocator);
	}

	@Override
	public void removeSchema(
			final Name typeName )
			throws IOException {
		this.removeSchema(typeName.getLocalPart());
	}

	@Override
	public void removeSchema(
			final String typeName )
			throws IOException {
		final DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
				StringUtils.stringToBinary(typeName)));
		if (adapter != null) {
			final String[] authorizations = getAuthorizationSPI().getAuthorizations();
			dataStore.delete(
					new QueryOptions(
							adapter,
							(PrimaryIndex) null,
							authorizations),
					new AdapterIdQuery(
							adapter.getAdapterId()));
			// TODO do we want to delete the adapter from the adapter store?
		}
	}

	/**
	 * Used to retrieve the TransactionStateDiff for this transaction.
	 * <p>
	 * 
	 * @param transaction
	 * @return GeoWaveTransactionState or null if subclass is handling
	 *         differences
	 * @throws IOException
	 */
	protected GeoWaveTransactionState getMyTransactionState(
			final Transaction transaction,
			final GeoWaveFeatureSource source )
			throws IOException {
		synchronized (transaction) {
			GeoWaveTransactionState state = null;
			if (transaction == Transaction.AUTO_COMMIT) {
				state = new GeoWaveAutoCommitTransactionState(
						source);
			}
			else {
				state = (GeoWaveTransactionState) transaction.getState(this);
				if (state == null) {
					state = new GeoWaveTransactionManagementState(
							transactionBufferSize,
							source.getComponents(),
							transaction,
							(LockingManagement) lockingManager);
					transaction.putState(
							this,
							state);
				}
			}
			return state;
		}
	}

	private List<PrimaryIndex> getPreferredIndex(
			final FeatureDataAdapter adapter ) {

		List<PrimaryIndex> currentSelections = preferredIndexes.get(adapter.getType().getName().toString());
		if (currentSelections != null) {
			return currentSelections;
		}

		currentSelections = new ArrayList<PrimaryIndex>(
				2);
		final List<String> indexNames = SimpleFeaturePrimaryIndexConfiguration.getIndexNames(adapter.getType());
		PrimaryIndex bestSelection = null;
		final boolean needTime = adapter.hasTemporalConstraints();

		try (CloseableIterator<Index<?, ?>> indices = indexStore.getIndices()) {
			boolean currentSelectionHasTime = false;
			while (indices.hasNext()) {
				final PrimaryIndex index = (PrimaryIndex) indices.next();

				if (!indexNames.isEmpty() && indexNames.contains(index.getId().getString())) {
					currentSelections.add(index);
				}
				@SuppressWarnings("rawtypes")
				final NumericDimensionField[] dims = index.getIndexModel().getDimensions();
				boolean hasLat = false;
				boolean hasLong = false;
				boolean hasTime = false;
				for (final NumericDimensionField<?> dim : dims) {
					hasLat |= dim instanceof LatitudeField;
					hasLong |= dim instanceof LongitudeField;
					hasTime |= dim instanceof TimeField;
				}

				// pick the first matching one or
				// pick the one does not match the required time constraints
				if (hasLat && hasLong) {
					if ((bestSelection == null) || (currentSelectionHasTime != needTime)) {
						bestSelection = index;
						currentSelectionHasTime = hasTime;
					}
				}
			}
			// at this point, preferredID is not found
			// only select the index if one has not been found or
			// the current selection. Not using temporal at this point.
			// temporal index should only be used if explicitly requested.
			if (bestSelection == null) {
				bestSelection = new SpatialDimensionalityTypeProvider().createPrimaryIndex();
			}
		}
		catch (final IOException ex) {
			LOGGER.error(
					"Cannot close index iterator.",
					ex);
		}

		if (currentSelections.isEmpty() && bestSelection != null) currentSelections.add(bestSelection);

		preferredIndexes.put(
				adapter.getType().getName().toString(),
				currentSelections);
		return currentSelections;
	}
}
