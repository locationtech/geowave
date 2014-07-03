package mil.nga.giat.geowave.gt.datastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.accumulo.AbstractAccumuloPersistence;
import mil.nga.giat.geowave.accumulo.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.gt.GeoWaveDataStore;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DefaultServiceInfo;
import org.geotools.data.EmptyFeatureWriter;
import org.geotools.data.FeatureListener;
import org.geotools.data.FeatureListenerManager;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureWriter;
import org.geotools.data.LockingManager;
import org.geotools.data.Query;
import org.geotools.data.ServiceInfo;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeImpl;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
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
public class GeoWaveGTDataStore implements
		DataStore
{
	/** Package logger */
	private final static Logger LOGGER = Logger.getLogger(GeoWaveGTDataStore.class);
	public static CoordinateReferenceSystem DEFAULT_CRS;
	static {
		try {
			DEFAULT_CRS = CRS.decode("EPSG:4326");
		}
		catch (final FactoryException e) {
			LOGGER.error(
					"Unable to decode EPSG:4326 CRS",
					e);
		}
	}

	private FeatureListenerManager listenerMgr = null;
	private AdapterStore adapterStore;
	protected GeoWaveDataStore dataStore;
	protected GeoWaveDataStore statsDataStore;
	protected AccumuloOperations statsOperations;

	public GeoWaveGTDataStore(
			final GeoWavePluginConfig config )
			throws AccumuloException,
			AccumuloSecurityException {
		listenerMgr = new FeatureListenerManager();
		init(config);
	}

	@Override
	public void dispose() {
		// TODO are there any native resources we need to dispose of
	}

	public void init(
			final GeoWavePluginConfig config )
			throws AccumuloException,
			AccumuloSecurityException {
		final AccumuloOperations operations = new BasicAccumuloOperations(
				config.getZookeeperServers(),
				config.getInstanceName(),
				config.getUserName(),
				config.getPassword(),
				config.getNamespace());
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);
		adapterStore = new AccumuloAdapterStore(
				operations);
		dataStore = new GeoWaveDataStore(
				indexStore,
				adapterStore,
				operations);

		statsOperations = new BasicAccumuloOperations(
				config.getZookeeperServers(),
				config.getInstanceName(),
				config.getUserName(),
				config.getPassword(),
				config.getNamespace() + "_stats");
		statsDataStore = new GeoWaveDataStore(
				statsOperations);
	}

	public void addListener(
			final FeatureSource<?, ?> src,
			final FeatureListener listener ) {
		listenerMgr.addFeatureListener(
				src,
				listener);
	}

	public void removeListener(
			final FeatureSource<?, ?> src,
			final FeatureListener listener ) {
		listenerMgr.removeFeatureListener(
				src,
				listener);
	}

	@Override
	public void createSchema(
			final SimpleFeatureType featureType ) {
		throw new UnsupportedOperationException(
				"Schema creation not supported");

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
	public FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(
			final Query query,
			final Transaction arg1 ) {
		final FeatureDataAdapter adapter = getAdapter(query.getTypeName());

		if (adapter == null) {
			LOGGER.warn("GeoWave Feature Data Adapter of type '" + query.getTypeName() + "' not found.  Cannot create Feature Reader.");
			return null;
		}
		final GeoWaveFeatureSource source = new GeoWaveFeatureSource(
				this,
				adapter);
		return source.getReaderInternal(query);
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

	@Override
	public SimpleFeatureSource getFeatureSource(
			final Name name )
			throws IOException {
		return getFeatureSource(name.getLocalPart());
	}

	@Override
	public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(
			final String arg0,
			final Transaction arg1 )
			throws IOException {
		return new EmptyFeatureWriter(
				new SimpleFeatureTypeBuilder().buildFeatureType());
	}

	@Override
	public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(
			final String arg0,
			final Filter arg1,
			final Transaction arg2 )
			throws IOException {
		return new EmptyFeatureWriter(
				new SimpleFeatureTypeBuilder().buildFeatureType());
	}

	@Override
	public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(
			final String arg0,
			final Transaction arg1 )
			throws IOException {
		return new EmptyFeatureWriter(
				new SimpleFeatureTypeBuilder().buildFeatureType());
	}

	@Override
	public LockingManager getLockingManager() {
		return null;
	}

	private FeatureDataAdapter getAdapter(
			final String typeName ) {
		final SimpleFeatureType statsFeatureType = getDefaultSchema(typeName);
		final FeatureDataAdapter featureAdapter;
		if (statsFeatureType != null) {
			featureAdapter = new FeatureDataAdapter(
					statsFeatureType);
		}
		else {
			final DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
					StringUtils.stringToBinary(typeName)));
			if ((adapter == null) || !(adapter instanceof FeatureDataAdapter)) {
				return null;
			}
			featureAdapter = (FeatureDataAdapter) adapter;
		}
		featureAdapter.setNamespace(null);
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
			return new SimpleFeatureTypeImpl(
					new NameImpl(
							type.getName().getNamespaceURI(),
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
			final Iterator<DataAdapter<?>> adapters = adapterStore.getAdapters();
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
		final Iterator<DataAdapter<?>> it = adapterStore.getAdapters();
		final List<String> typeNames = new ArrayList<String>();
		typeNames.addAll(Arrays.asList(getDefaultTypeNames()));
		while (it.hasNext()) {
			final DataAdapter<?> adapter = it.next();
			if (adapter instanceof FeatureDataAdapter) {
				typeNames.add(((FeatureDataAdapter) adapter).getType().getTypeName());
			}
		}
		return typeNames.toArray(new String[] {});
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
		throw new UnsupportedOperationException(
				"Schema modification not supported");
	}

	@Override
	public void removeSchema(
			final Name typeName )
			throws IOException {}

	@Override
	public void removeSchema(
			final String typeName )
			throws IOException {}
}
