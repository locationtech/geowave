package mil.nga.giat.geowave.vector.plugin;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AbstractAccumuloPersistence;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.store.dimension.DimensionField;
import mil.nga.giat.geowave.store.dimension.LatitudeField;
import mil.nga.giat.geowave.store.dimension.LongitudeField;
import mil.nga.giat.geowave.store.dimension.TimeField;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.AccumuloDataStatisticsStoreExt;
import mil.nga.giat.geowave.vector.VectorDataStore;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.vector.auth.AuthorizationSPI;
import mil.nga.giat.geowave.vector.auth.EmptyAuthorizationProvider;
import mil.nga.giat.geowave.vector.plugin.lock.LockingManagement;
import mil.nga.giat.geowave.vector.plugin.lock.MemoryLockManager;
import mil.nga.giat.geowave.vector.plugin.transaction.GeoWaveAutoCommitTransactionState;
import mil.nga.giat.geowave.vector.plugin.transaction.GeoWaveEmptyTransaction;
import mil.nga.giat.geowave.vector.plugin.transaction.GeoWaveTransactionManagementState;
import mil.nga.giat.geowave.vector.plugin.transaction.GeoWaveTransactionState;
import mil.nga.giat.geowave.vector.plugin.visibility.ColumnVisibilityManagement;
import mil.nga.giat.geowave.vector.plugin.visibility.VisibilityManagementHelper;
import mil.nga.giat.geowave.vector.transaction.TransactionNotification;
import mil.nga.giat.geowave.vector.transaction.TransactionsAllocater;
import mil.nga.giat.geowave.vector.transaction.ZooKeeperTransactionsAllocater;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.geotools.data.AbstractDataStore;
import org.geotools.data.DataSourceException;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultServiceInfo;
import org.geotools.data.EmptyFeatureReader;
import org.geotools.data.FeatureListener;
import org.geotools.data.FeatureListenerManager;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureWriter;
import org.geotools.data.FilteringFeatureReader;
import org.geotools.data.FilteringFeatureWriter;
import org.geotools.data.MaxFeatureReader;
import org.geotools.data.Query;
import org.geotools.data.ReTypeFeatureReader;
import org.geotools.data.ServiceInfo;
import org.geotools.data.Transaction;
import org.geotools.data.TransactionStateDiff;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureTypeImpl;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * This is the main entry point for exposing GeoWave as a DataStore to GeoTools.
 * GeoTools uses Java SPI (see
 * META-INF/services/org.geotools.data.DataStoreFactorySpi) to inject this
 * datastore.
 * 
 */
public class GeoWaveGTDataStore extends
		AbstractDataStore implements
		DataStore,
		TransactionNotification
{
	/** Package logger */
	private final static Logger LOGGER = Logger.getLogger(GeoWaveGTDataStore.class);
	public static CoordinateReferenceSystem DEFAULT_CRS;
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
		}
	}

	private FeatureListenerManager listenerManager = null;
	private AdapterStore adapterStore;
	protected VectorDataStore dataStore;
	protected VectorDataStore statsDataStore;
	protected AccumuloOperations statsOperations;
	protected AccumuloOperations storeOperations;
	private final Map<String, Index> preferredIndexes = new ConcurrentHashMap<String, Index>();
	private final ColumnVisibilityManagement<SimpleFeature> visibilityManagement = VisibilityManagementHelper.loadVisibilityManagement();

	private final AuthorizationSPI authorizationSPI;
	final private TransactionsAllocater transactionsAllocater;
	private URI featureNameSpaceURI;

	/**
	 * Manages InProcess locks for FeatureLocking implementations.
	 */
	private final LockingManagement lockingManager;

	public GeoWaveGTDataStore(
			final TransactionsAllocater transactionsAllocater ) {
		listenerManager = new FeatureListenerManager();
		this.transactionsAllocater = transactionsAllocater;
		lockingManager = new MemoryLockManager(
				"default");
		authorizationSPI = new EmptyAuthorizationProvider();
	}

	public GeoWaveGTDataStore(
			final TransactionsAllocater transactionsAllocater,
			final AuthorizationSPI authSPI ) {
		listenerManager = new FeatureListenerManager();
		authorizationSPI = authSPI;
		lockingManager = new MemoryLockManager(
				"default");
		this.transactionsAllocater = transactionsAllocater;
	}

	public GeoWaveGTDataStore(
			final GeoWavePluginConfig config )
			throws IOException,
			AccumuloException,
			AccumuloSecurityException {
		listenerManager = new FeatureListenerManager();

		lockingManager = config.getLockingManagementFactory().createLockingManager(
				config);
		authorizationSPI = config.getAuthorizationFactory().create(
				config.getAuthorizationURL());
		init(config);
		transactionsAllocater = new ZooKeeperTransactionsAllocater(
				config.getZookeeperServers(),
				"gt",
				this);

		featureNameSpaceURI = config.getFeatureNamespace();

	}

	@Override
	public void dispose() {
		// TODO are there any native resources we need to dispose of
	}

	public AuthorizationSPI getAuthorizationSPI() {
		return authorizationSPI;
	}

	public ColumnVisibilityManagement<SimpleFeature> getVisibilityManagement() {
		return visibilityManagement;
	}

	public FeatureListenerManager getListenerManager() {
		return listenerManager;
	}

	protected void setListenerManager(
			final FeatureListenerManager listenerMgr ) {
		listenerManager = listenerMgr;
	}

	protected AdapterStore getAdapterStore() {
		return adapterStore;
	}

	protected void setAdapterStore(
			final AdapterStore adapterStore ) {
		this.adapterStore = adapterStore;
	}

	protected VectorDataStore getDataStore() {
		return dataStore;
	}

	protected void setDataStore(
			final VectorDataStore dataStore ) {
		this.dataStore = dataStore;
	}

	protected VectorDataStore getStatsDataStore() {
		return statsDataStore;
	}

	protected void setStatsDataStore(
			final VectorDataStore statsDataStore ) {
		this.statsDataStore = statsDataStore;
	}

	protected AccumuloOperations getStatsOperations() {
		return statsOperations;
	}

	protected void setStoreOperations(
			final AccumuloOperations storeOperations ) {
		this.storeOperations = storeOperations;
	}

	protected void setStatsOperations(
			final AccumuloOperations statsOperations ) {
		this.statsOperations = statsOperations;
	}

	public void init(
			final GeoWavePluginConfig config )
			throws AccumuloException,
			AccumuloSecurityException {
		storeOperations = new BasicAccumuloOperations(
				config.getZookeeperServers(),
				config.getInstanceName(),
				config.getUserName(),
				config.getPassword(),
				config.getAccumuloNamespace());
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				storeOperations);

		final DataStatisticsStore statisticsStore = new AccumuloDataStatisticsStoreExt(
				storeOperations);
		adapterStore = new AccumuloAdapterStore(
				storeOperations);
		dataStore = new VectorDataStore(
				indexStore,
				adapterStore,
				statisticsStore,
				storeOperations);

		statsOperations = new BasicAccumuloOperations(
				config.getZookeeperServers(),
				config.getInstanceName(),
				config.getUserName(),
				config.getPassword(),
				config.getAccumuloNamespace() + "_stats");

		statsDataStore = new VectorDataStore(
				statsOperations);
	}

	protected Index getIndex(
			final FeatureDataAdapter adapter ) {
		return getPreferredIndex(adapter);
	}

	public void addListener(
			final FeatureSource<?, ?> src,
			final FeatureListener listener ) {
		listenerManager.addFeatureListener(
				src,
				listener);

	}

	public void removeListener(
			final FeatureSource<?, ?> src,
			final FeatureListener listener ) {
		listenerManager.removeFeatureListener(
				src,
				listener);
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
				getVisibilityManagement());
		if (featureNameSpaceURI != null) {
			adapter.setNamespace(featureNameSpaceURI.toString());
		}

		adapterStore.addAdapter(adapter);
		getPreferredIndex(adapter);
	}

	@Override
	public ServiceInfo getInfo() {
		final DefaultServiceInfo info = new DefaultServiceInfo();
		info.setTitle("GeoWave Data Store");
		info.setDescription("Features from GeoWave");
		return info;
	}

	@Override
	public List<Name> getNames() {
		final String[] typeNames = getTypeNames();
		final List<Name> names = new ArrayList<Name>(
				typeNames.length);
		for (final String typeName : typeNames) {
			names.add(new NameImpl(
					typeName));
		}
		return names;
	}

	@Override
	public SimpleFeatureType getSchema(
			final Name name ) {
		return getSchema(name.getLocalPart());

	}

	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(
			final String typeName,
			final Query query )
			throws IOException {
		final FeatureDataAdapter adapter = getAdapter(query.getTypeName());

		if (adapter == null) {
			LOGGER.warn("GeoWave Feature Data Adapter of type '" + query.getTypeName() + "' not found.  Cannot create Feature Reader.");
			return null;
		}
		final GeoWaveFeatureSource source = new GeoWaveFeatureSource(
				this,
				adapter);
		return source.getReaderInternal(
				query,
				new GeoWaveEmptyTransaction(
						source.getComponents()));
	}

	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(
			final String typeName )
			throws IOException {
		throw new UnsupportedOperationException(
				"Should not get here");
	}

	@Override
	public SimpleFeatureSource getFeatureSource(
			final String typeName ) {

		final FeatureDataAdapter adapter = getAdapter(typeName);
		if (adapter == null) {
			LOGGER.warn("GeoWave Feature Data Adapter of type '" + typeName + "' not found.  Cannot create Feature Source.");
			return null;
		}
		return new GeoWaveFeatureSource(
				this,
				adapter);
	}

	public TransactionsAllocater getTransactionsAllocater() {
		return transactionsAllocater;
	}

	@Override
	public SimpleFeatureSource getFeatureSource(
			final Name name )
			throws IOException {
		return getFeatureSource(name.getLocalPart());
	}

	public SimpleFeatureSource getFeatureSource(
			final Name name,
			final Transaction transaction )
			throws IOException {
		return getFeatureSource(name.getLocalPart());
	}

	@Override
	protected FeatureWriter<SimpleFeatureType, SimpleFeature> createFeatureWriter(
			final String typeName,
			final Transaction transaction )
			throws IOException {
		return getFeatureWriter(
				typeName,
				Filter.INCLUDE,
				transaction);

	}

	@Override
	public LockingManagement getLockingManager() {
		return lockingManager;
	}

	/**
	 * Need to override this to handle the custom transaction state. This a
	 * really bummer. It would have been easier if GeoTools had exposed
	 * 'applyDiff' on the TransactionDiffState.
	 */
	@Override
	public FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(
			final Query query,
			final Transaction transaction )
			throws IOException {
		final Filter filter = query.getFilter();
		final String typeName = query.getTypeName();
		final String propertyNames[] = query.getPropertyNames();

		if (filter == null) {
			throw new NullPointerException(
					"getFeatureReader requires Filter: " + "did you mean Filter.INCLUDE?");
		}
		if (typeName == null) {
			throw new NullPointerException(
					"getFeatureReader requires typeName: " + "use getTypeNames() for a list of available types");
		}
		if (transaction == null) {
			throw new NullPointerException(
					"getFeatureReader requires Transaction: " + "did you mean to use Transaction.AUTO_COMMIT?");
		}
		SimpleFeatureType featureType = getSchema(query.getTypeName());
		final GeoWaveFeatureSource source = (GeoWaveFeatureSource) getFeatureSource(typeName);

		if ((propertyNames != null) || (query.getCoordinateSystem() != null)) {
			try {
				featureType = DataUtilities.createSubType(
						featureType,
						propertyNames,
						query.getCoordinateSystem());
			}
			catch (final SchemaException e) {
				throw new DataSourceException(
						"Could not create Feature Type for query",
						e);

			}
		}
		if ((filter == Filter.EXCLUDE) || filter.equals(Filter.EXCLUDE)) {
			return new EmptyFeatureReader<SimpleFeatureType, SimpleFeature>(
					featureType);
		}

		final GeoWaveTransactionState state = getMyTransactionState(
				transaction,
				source);

		FeatureReader<SimpleFeatureType, SimpleFeature> reader = source.getReaderInternal(
				query,
				state.getGeoWaveTransaction(typeName));

		if (!filter.equals(Filter.INCLUDE)) {
			reader = new FilteringFeatureReader<SimpleFeatureType, SimpleFeature>(
					reader,
					filter);
		}

		if (!featureType.equals(reader.getFeatureType())) {
			reader = new ReTypeFeatureReader(
					reader,
					featureType,
					false);
		}

		if (query.getMaxFeatures() != Query.DEFAULT_MAX) {
			reader = new MaxFeatureReader<SimpleFeatureType, SimpleFeature>(
					reader,
					query.getMaxFeatures());
		}

		return reader;
	}

	@Override
	public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(
			final String typeName,
			final Filter filter,
			final Transaction transaction )
			throws IOException {

		final GeoWaveFeatureSource source = (GeoWaveFeatureSource) getFeatureSource(typeName);

		if (filter == null) {
			throw new NullPointerException(
					"getFeatureReader requires Filter: " + "did you mean Filter.INCLUDE?");
		}

		if (transaction == null) {
			throw new NullPointerException(
					"getFeatureWriter requires Transaction: " + "did you mean to use Transaction.AUTO_COMMIT?");
		}

		final GeoWaveTransactionState state = getMyTransactionState(
				transaction,
				source);

		if (filter == Filter.EXCLUDE) {
			return source.getWriterInternal(state.getGeoWaveTransaction(typeName));
		}

		FeatureWriter<SimpleFeatureType, SimpleFeature> writer;

		writer = source.getWriterInternal(
				state.getGeoWaveTransaction(typeName),
				filter);

		if (filter != Filter.INCLUDE) {
			writer = new FilteringFeatureWriter(
					writer,
					filter);
		}

		return writer;
	}

	@Override
	public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(
			final String typeName,
			final Transaction transaction )
			throws IOException {
		return this.getFeatureWriter(
				typeName,
				Filter.EXCLUDE,
				transaction);
	}

	private FeatureDataAdapter getAdapter(
			final String typeName ) {
		final SimpleFeatureType statsFeatureType = getDefaultSchema(typeName);
		final FeatureDataAdapter featureAdapter;
		if (statsFeatureType != null) {
			featureAdapter = new FeatureDataAdapter(
					statsFeatureType,
					getVisibilityManagement());
		}
		else {
			final DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
					StringUtils.stringToBinary(typeName)));
			if ((adapter == null) || !(adapter instanceof FeatureDataAdapter)) {
				return null;
			}
			featureAdapter = (FeatureDataAdapter) adapter;
		}
		if (featureNameSpaceURI != null) {
			featureAdapter.setNamespace(featureNameSpaceURI.toString());
		}
		// else
		// featureAdapter.setNamespace(null);
		return featureAdapter;
	}

	@Override
	public SimpleFeatureType getSchema(
			final String typeName ) {
		final SimpleFeatureType type = getDefaultSchema(typeName);
		if (type != null) {
			return type;
		}
		final FeatureDataAdapter adapter = getAdapter(typeName);
		if (adapter == null) {
			return null;
		}
		return adapter.getType();
	}

	private SimpleFeatureType getDefaultSchema(
			final String typeName ) {
		// TODO add some basic "global" types such as all spatial, or all
		// spatial temporal
		final FeatureDataAdapter adapter = getStatsAdapter(typeName);
		if (adapter != null) {
			final SimpleFeatureType type = adapter.getType();
			final String nameSpace = featureNameSpaceURI != null ? featureNameSpaceURI.toString() : type.getName().getNamespaceURI();

			return new SimpleFeatureTypeImpl(
					new NameImpl(
							nameSpace,
							type.getName().getSeparator(),
							typeName),
					type.getAttributeDescriptors(),
					type.getGeometryDescriptor(),
					type.isAbstract(),
					type.getRestrictions(),
					type.getSuper(),
					type.getDescription());
		}
		return null;
	}

	protected FeatureDataAdapter getStatsAdapter(
			final String typeName ) {
		// TODO add some basic "global" types such as all spatial, or all
		// spatial temporal
		if (typeName.startsWith("heatmap_")) {
			if (statsOperations.tableExists(AbstractAccumuloPersistence.METADATA_TABLE)) {
				final AdapterStore adapterStore = new AccumuloAdapterStore(
						statsOperations);
				final DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
						StringUtils.stringToBinary("l0_stats" + typeName.substring(8))));
				if (adapter instanceof FeatureDataAdapter) {
					return (FeatureDataAdapter) adapter;
				}
			}
		}
		return null;
	}

	private String[] getDefaultTypeNames() {
		if (statsOperations.tableExists(AbstractAccumuloPersistence.METADATA_TABLE)) {
			final AdapterStore adapterStore = new AccumuloAdapterStore(
					statsOperations);
			try (final CloseableIterator<DataAdapter<?>> adapters = adapterStore.getAdapters()) {
				final Set<String> typeNames = new HashSet<String>();
				while (adapters.hasNext()) {
					final DataAdapter<?> adapter = adapters.next();
					typeNames.add(StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()));
				}
				final Set<String> statsNames = getStatsNamesFromTypeNames(typeNames);
				final String[] defaultTypeNames = new String[statsNames.size()];
				final Iterator<String> statsIt = statsNames.iterator();
				int i = 0;
				while (statsIt.hasNext()) {
					defaultTypeNames[i++] = "heatmap_" + statsIt.next();
				}
				return defaultTypeNames;
			}
			catch (final IOException e) {
				LOGGER.warn(
						"unable to close adapters store iterator",
						e);
			}
		}
		return new String[] {};
	}

	private static Set<String> getStatsNamesFromTypeNames(
			final Set<String> typeNames ) {
		final Set<String> statsNames = new HashSet<String>();
		for (final String typeName : typeNames) {
			try {
				statsNames.add(typeName.substring(typeName.indexOf("_stats") + 6));
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to parse name from stats adapter store",
						e);
			}
		}
		return statsNames;
	}

	@Override
	public String[] getTypeNames() {
		try (final CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters()) {
			final List<String> typeNames = new ArrayList<String>();
			typeNames.addAll(Arrays.asList(getDefaultTypeNames()));
			while (it.hasNext()) {
				final DataAdapter<?> adapter = it.next();
				if (adapter instanceof FeatureDataAdapter) {
					typeNames.add(((FeatureDataAdapter) adapter).getType().getTypeName());
				}
			}
			return typeNames.toArray(new String[typeNames.size()]);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"unable to close adapter store",
					e);
			return new String[] {};
		}
	}

	@Override
	public void updateSchema(
			final Name name,
			final SimpleFeatureType featureType ) {
		updateSchema(
				name.getLocalPart(),
				featureType);
	}

	@Override
	public void updateSchema(
			final String typeName,
			final SimpleFeatureType featureType ) {
		if (featureType.getGeometryDescriptor() == null) {
			throw new UnsupportedOperationException(
					"Schema modification not supported");
		}
		final FeatureDataAdapter oldAdaptor = getAdapter(typeName);
		final SimpleFeatureType oldFeatureType = oldAdaptor.getType();
		for (final AttributeDescriptor descriptor : oldFeatureType.getAttributeDescriptors()) {
			final AttributeDescriptor newDescriptor = featureType.getDescriptor(descriptor.getLocalName());
			if ((newDescriptor == null) || !newDescriptor.getType().equals(
					descriptor.getType())) {
				throw new UnsupportedOperationException(
						"Removal or changing the type of schema attributes is not supported");
			}
		}
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				featureType,
				getVisibilityManagement());

		adapterStore.addAdapter(adapter);
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
			try (CloseableIterator<Index> indicesIt = dataStore.getIndices()) {
				while (indicesIt.hasNext()) {
					dataStore.deleteEntries(
							adapter,
							indicesIt.next(),
							authorizations);
				}
			}
			catch (final IOException ex) {
				LOGGER.error(
						"Cannot close index iterator.",
						ex);
			}
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

							source.getComponents(),
							transaction,
							lockingManager);
					transaction.putState(
							this,
							state);
				}
			}
			return state;
		}
	}

	@Override
	protected TransactionStateDiff state(
			final Transaction transaction ) {
		return null;
	}

	private Index getPreferredIndex(
			final FeatureDataAdapter adapter ) {

		Index currentSelection = preferredIndexes.get(adapter.getType().getName().toString());
		if (currentSelection != null) {
			return currentSelection;
		}

		final boolean needTime = adapter.hasTemporalConstraints();

		try (CloseableIterator<Index> indices = dataStore.getIndices()) {
			boolean currentSelectionHasTime = false;
			while (indices.hasNext()) {
				final Index index = indices.next();
				@SuppressWarnings("rawtypes")
				final DimensionField[] dims = index.getIndexModel().getDimensions();
				boolean hasLat = false;
				boolean hasLong = false;
				boolean hasTime = false;
				for (final DimensionField<?> dim : dims) {
					hasLat |= dim instanceof LatitudeField;
					hasLong |= dim instanceof LongitudeField;
					hasTime |= dim instanceof TimeField;
				}

				// pick the first matching one or
				// pick the one does not match the required time constraints
				if (hasLat && hasLong) {
					if ((currentSelection == null) || (currentSelectionHasTime != needTime)) {
						currentSelection = index;
						currentSelectionHasTime = hasTime;
					}
				}
			}
			// at this point, preferredID is not found
			// only select the index if one has not been found or
			// the current selection. Not using temporal at this point.
			// temporal index should only be used if explicitly requested.
			if (currentSelection == null) {
				currentSelection = IndexType.SPATIAL_VECTOR.createDefaultIndex();
			}
		}
		catch (final IOException ex) {
			LOGGER.error(
					"Cannot close index iterator.",
					ex);
		}

		preferredIndexes.put(
				adapter.getType().getName().toString(),
				currentSelection);
		return currentSelection;
	}

	@Override
	public boolean transactionCreated(
			final String clientID,
			final String txID ) {
		try {
			((BasicAccumuloOperations) storeOperations).insureAuthorization(txID);
			return true;
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Cannot add transaction id as an authorization.",
					ex);
			return false;
		}
	}

	Name getTypeName(
			final String typeName ) {
		return new NameImpl(
				featureNameSpaceURI.toString(),
				typeName);
	}

	private static void printHelp(
			final Options options ) {
		final HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(
				"GeoWaveGTDateStore",
				"\nOptions:",
				options,
				"");
	}

	public static void main(
			final String args[] )
			throws ParseException,
			GeoWavePluginException,
			IOException,
			AccumuloException,
			AccumuloSecurityException {
		final Options options = new Options();
		final OptionGroup baseOptionGroup = new OptionGroup();
		baseOptionGroup.setRequired(false);
		baseOptionGroup.addOption(new Option(
				"h",
				"help",
				false,
				"Display help"));
		baseOptionGroup.addOption(new Option(
				"m",
				"maximum",
				true,
				"Maximum number of simulataneous transactions"));
		options.addOptionGroup(baseOptionGroup);
		GeoWavePluginConfig.applyOptions(options);
		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				options,
				args);
		if (commandLine.hasOption("h")) {
			printHelp(options);
			System.exit(0);
		}
		else {
			try {
				final GeoWavePluginConfig plugin = GeoWavePluginConfig.buildFromOptions(commandLine);
				final int maximum = Integer.parseInt(commandLine.getOptionValue('m'));
				final GeoWaveGTDataStore dataStore = new GeoWaveGTDataStore(
						plugin);
				((ZooKeeperTransactionsAllocater) dataStore.transactionsAllocater).preallocateTransactionIDs(maximum);
			}
			catch (final Exception ex) {
				LOGGER.error(
						"Failed to pre-allocate transaction ID set",
						ex);
				System.exit(-1);
			}
		}
	}
}
