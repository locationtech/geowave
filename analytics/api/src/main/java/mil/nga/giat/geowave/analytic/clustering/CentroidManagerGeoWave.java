package mil.nga.giat.geowave.analytic.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.AccumuloCqlConstraintsQuery;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.AnalyticFeature.ClusterFeatureAttribute;
import mil.nga.giat.geowave.analytic.clustering.exception.MatchingCentroidNotFoundException;
import mil.nga.giat.geowave.analytic.db.BasicAccumuloOperationsFactory;
import mil.nga.giat.geowave.analytic.db.DirectBasicAccumuloOperationsFactory;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.Option;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.geotools.filter.FilterFactoryImpl;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * 
 * Manages the population of centroids by group id and batch id.
 * 
 * Properties:
 * 
 * @formatter:off
 * 
 *                "CentroidManagerGeoWave.Centroid.WrapperFactoryClass" -
 *                {@link AnalyticItemWrapperFactory} to extract wrap spatial
 *                objects with Centroid management function
 * 
 *                "CentroidManagerGeoWave.Centroid.DataTypeId" -> The data type
 *                ID of the centroid simple feature
 * 
 *                "CentroidManagerGeoWave.Centroid.IndexId" -> The GeoWave index
 *                ID of the centroid simple feature
 * 
 *                "CentroidManagerGeoWave.Global.BatchId" -> Batch ID for
 *                updates
 * 
 *                "CentroidManagerGeoWave.Global.Zookeeper" -> Zookeeper URL
 * 
 *                "CentroidManagerGeoWave.Global.AccumuloInstance" -> Accumulo
 *                Instance Name
 * 
 *                "CentroidManagerGeoWave.Global.AccumuloUser" -> Accumulo User
 *                name
 * 
 *                "CentroidManagerGeoWave.Global.AccumuloPassword" -> Accumulo
 *                Password
 * 
 *                "CentroidManagerGeoWave.Global.AccumuloNamespace" -> Accumulo
 *                Table Namespace
 * 
 *                "CentroidManagerGeoWave.Common.AccumuloConnectFactory" ->
 *                {@link BasicAccumuloOperationsFactory}
 * 
 * @formatter:on
 * 
 * @param <T>
 *            The item type used to represent a centroid.
 */
public class CentroidManagerGeoWave<T> implements
		CentroidManager<T>
{
	final static Logger LOGGER = LoggerFactory.getLogger(CentroidManagerGeoWave.class);

	private final String centroidDataTypeId;
	private final String batchId;
	private int level = 0;

	private AccumuloOperations basicAccumuloOperations;
	private final AnalyticItemWrapperFactory<T> centroidFactory;
	@SuppressWarnings("rawtypes")
	private final DataAdapter adapter;

	private final AccumuloDataStore dataStore;
	private final AccumuloIndexStore indexStore;
	private final AccumuloAdapterStore adapterStore;
	private final Index index;

	public CentroidManagerGeoWave(
			final BasicAccumuloOperations basicAccumuloOperations,
			final AnalyticItemWrapperFactory<T> centroidFactory,
			final String centroidDataTypeId,
			final String indexId,
			final String batchId,
			final int level )
			throws AccumuloException,
			AccumuloSecurityException {
		this.basicAccumuloOperations = basicAccumuloOperations;
		this.centroidFactory = centroidFactory;
		this.centroidDataTypeId = centroidDataTypeId;
		this.level = level;
		this.batchId = batchId;
		dataStore = new AccumuloDataStore(
				basicAccumuloOperations);
		indexStore = new AccumuloIndexStore(
				basicAccumuloOperations);
		index = indexStore.getIndex(new ByteArrayId(
				StringUtils.stringToBinary(indexId)));
		adapterStore = new AccumuloAdapterStore(
				basicAccumuloOperations);
		adapter = adapterStore.getAdapter(new ByteArrayId(
				StringUtils.stringToBinary(centroidDataTypeId)));
	}

	public CentroidManagerGeoWave(
			final String zookeeperUrl,
			final String instanceName,
			final String userName,
			final String password,
			final String tableNamespace,
			final AnalyticItemWrapperFactory<T> centroidFactory,
			final String centroidDataTypeId,
			final String indexId,
			final String batchId,
			final int level )
			throws AccumuloException,
			AccumuloSecurityException {
		this.basicAccumuloOperations = new BasicAccumuloOperations(
				zookeeperUrl,
				instanceName,
				userName,
				password,
				tableNamespace);
		this.centroidFactory = centroidFactory;
		this.centroidDataTypeId = centroidDataTypeId;
		this.batchId = batchId;
		this.level = level;
		dataStore = new AccumuloDataStore(
				basicAccumuloOperations);
		indexStore = new AccumuloIndexStore(
				basicAccumuloOperations);
		index = indexStore.getIndex(new ByteArrayId(
				StringUtils.stringToBinary(indexId)));
		adapterStore = new AccumuloAdapterStore(
				basicAccumuloOperations);
		adapter = adapterStore.getAdapter(new ByteArrayId(
				StringUtils.stringToBinary(centroidDataTypeId)));

	}

	@SuppressWarnings("unchecked")
	public CentroidManagerGeoWave(
			final ConfigurationWrapper context )
			throws AccumuloException,
			IOException,
			AccumuloSecurityException {

		try {
			centroidFactory = context.getInstance(
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					this.getClass(),
					AnalyticItemWrapperFactory.class,
					SimpleFeatureItemWrapperFactory.class);
			centroidFactory.initialize(context);

		}
		catch (final Exception e1) {
			LOGGER.error("Cannot instantiate " + GeoWaveConfiguratorBase.enumToConfKey(
					this.getClass(),
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS));
			throw new IOException(
					e1);
		}

		this.level = context.getInt(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				this.getClass(),
				1);

		centroidDataTypeId = context.getString(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				this.getClass(),
				"centroid");

		batchId = context.getString(
				GlobalParameters.Global.BATCH_ID,
				this.getClass(),
				Long.toString(Calendar.getInstance().getTime().getTime()));

		final String indexId = context.getString(
				CentroidParameters.Centroid.INDEX_ID,
				this.getClass(),
				IndexType.SPATIAL_VECTOR.getDefaultId());

		try {
			final String zk = context.getString(
					GlobalParameters.Global.ZOOKEEKER,
					this.getClass(),
					"localhost:2181");

			basicAccumuloOperations = context.getInstance(
					CommonParameters.Common.ACCUMULO_CONNECT_FACTORY,
					this.getClass(),
					BasicAccumuloOperationsFactory.class,
					DirectBasicAccumuloOperationsFactory.class).build(
					zk,
					context.getString(
							GlobalParameters.Global.ACCUMULO_INSTANCE,
							this.getClass(),
							""),
					context.getString(
							GlobalParameters.Global.ACCUMULO_USER,
							this.getClass(),
							"root"),
					context.getString(
							GlobalParameters.Global.ACCUMULO_PASSWORD,
							this.getClass(),
							""),
					context.getString(
							GlobalParameters.Global.ACCUMULO_NAMESPACE,
							this.getClass(),
							""));

		}
		catch (final Exception e) {
			LOGGER.error("Cannot instantiate " + GeoWaveConfiguratorBase.enumToConfKey(
					this.getClass(),
					CommonParameters.Common.ACCUMULO_CONNECT_FACTORY));
			throw new IOException(
					e.getLocalizedMessage(),
					e);
		}
		dataStore = new AccumuloDataStore(
				basicAccumuloOperations);
		indexStore = new AccumuloIndexStore(
				basicAccumuloOperations);
		index = indexStore.getIndex(new ByteArrayId(
				StringUtils.stringToBinary(indexId)));
		adapterStore = new AccumuloAdapterStore(
				basicAccumuloOperations);
		adapter = adapterStore.getAdapter(new ByteArrayId(
				StringUtils.stringToBinary(centroidDataTypeId)));
	}

	@SuppressWarnings("unchecked")
	public CentroidManagerGeoWave(
			final PropertyManagement runTimeProperties )
			throws AccumuloException,
			IOException,
			AccumuloSecurityException {
		try {
			this.centroidFactory = (AnalyticItemWrapperFactory<T>) runTimeProperties.getClassInstance(
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS.getBaseClass(),
					SimpleFeatureItemWrapperFactory.class);
		}
		catch (InstantiationException e) {
			throw new IOException(
					e.getLocalizedMessage(),
					e);
		}
		this.centroidDataTypeId = runTimeProperties.getPropertyAsString(CentroidParameters.Centroid.DATA_TYPE_ID);
		final String indexId = runTimeProperties.getPropertyAsString(CentroidParameters.Centroid.INDEX_ID);
		this.batchId = runTimeProperties.getPropertyAsString(
				GlobalParameters.Global.BATCH_ID,
				Long.toString(Calendar.getInstance().getTime().getTime()));
		this.level = runTimeProperties.getPropertyAsInt(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				1);

		try {
			basicAccumuloOperations = runTimeProperties.getInstance(
					CommonParameters.Common.ACCUMULO_CONNECT_FACTORY,
					this.getClass(),
					BasicAccumuloOperationsFactory.class,
					DirectBasicAccumuloOperationsFactory.class).build(
					runTimeProperties.getPropertyAsString(GlobalParameters.Global.ZOOKEEKER),
					runTimeProperties.getPropertyAsString(GlobalParameters.Global.ACCUMULO_INSTANCE),
					runTimeProperties.getPropertyAsString(GlobalParameters.Global.ACCUMULO_USER),
					runTimeProperties.getPropertyAsString(GlobalParameters.Global.ACCUMULO_PASSWORD),
					runTimeProperties.getPropertyAsString(GlobalParameters.Global.ACCUMULO_NAMESPACE));
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new IOException(
					e.getLocalizedMessage(),
					e);
		}

		dataStore = new AccumuloDataStore(
				basicAccumuloOperations);
		indexStore = new AccumuloIndexStore(
				basicAccumuloOperations);
		index = indexStore.getIndex(new ByteArrayId(
				StringUtils.stringToBinary(indexId)));
		adapterStore = new AccumuloAdapterStore(
				basicAccumuloOperations);
		adapter = adapterStore.getAdapter(new ByteArrayId(
				StringUtils.stringToBinary(centroidDataTypeId)));
	}

	/**
	 * Creates a new centroid based on the old centroid with new coordinates and
	 * dimension values
	 * 
	 * @param feature
	 * @param coordinate
	 * @param extraNames
	 * @param extraValues
	 * @return
	 */
	@Override
	public AnalyticItemWrapper<T> createNextCentroid(
			final T feature,
			final String groupID,
			final Coordinate coordinate,
			final String[] extraNames,
			final double[] extraValues ) {
		return centroidFactory.createNextItem(
				feature,
				groupID,
				coordinate,
				extraNames,
				extraValues);
	}

	private final int capacity = 100;
	private final LRUMap groupToCentroid = new LRUMap(
			capacity);

	@Override
	public void clear() {
		groupToCentroid.clear();
	}

	@Override
	public void delete(
			final String[] dataIds )
			throws IOException {
		for (final String dataId : dataIds) {
			if (dataId != null) {
				dataStore.deleteEntry(
						index,
						new ByteArrayId(
								StringUtils.stringToBinary(dataId)),
						new ByteArrayId(
								StringUtils.stringToBinary(centroidDataTypeId)));
			}
		}
	}

	@Override
	public List<String> getAllCentroidGroups()
			throws IOException {
		final List<String> groups = new ArrayList<String>();
		final CloseableIterator<T> it = getRawCentroids(
				this.batchId,
				null);
		while (it.hasNext()) {
			final AnalyticItemWrapper<T> item = centroidFactory.create(it.next());
			final String groupID = item.getGroupID();
			int pos = groups.indexOf(groupID);
			if (pos < 0) {
				pos = groups.size();
				groups.add(groupID);
			}
			// cache the first set
			if (pos < capacity) {
				getCentroidsForGroup(groupID);
			}
		}
		it.close();
		return groups;
	}

	@Override
	public List<AnalyticItemWrapper<T>> getCentroidsForGroup(
			final String groupID )
			throws IOException {
		return getCentroidsForGroup(
				this.batchId,
				groupID);
	}

	@Override
	public List<AnalyticItemWrapper<T>> getCentroidsForGroup(
			final String batchID,
			final String groupID )
			throws IOException {
		final String lookupGroup = (groupID == null) ? "##" : groupID;

		final Pair<String, String> gid = Pair.of(
				batchID,
				lookupGroup);
		@SuppressWarnings("unchecked")
		List<AnalyticItemWrapper<T>> centroids = (List<AnalyticItemWrapper<T>>) groupToCentroid.get(gid);
		if (centroids == null) {
			centroids = groupID == null ? loadCentroids(
					batchID,
					null) : loadCentroids(
					batchID,
					groupID);
			groupToCentroid.put(
					gid,
					centroids);
		}
		return centroids;
	}

	public AnalyticItemWrapper<T> getCentroidById(
			final String id,
			final String groupID )
			throws IOException,
			MatchingCentroidNotFoundException {
		for (final AnalyticItemWrapper<T> centroid : this.getCentroidsForGroup(groupID)) {
			if (centroid.getID().equals(
					id)) {
				return centroid;
			}
		}
		throw new MatchingCentroidNotFoundException(
				id);
	}

	private List<AnalyticItemWrapper<T>> loadCentroids(
			final String batchID,
			final String groupID )
			throws IOException {
		LOGGER.info("Extracting centroids for " + batchID);
		final List<AnalyticItemWrapper<T>> centroids = new ArrayList<AnalyticItemWrapper<T>>();
		try {

			CloseableIterator<T> it = null;

			try {
				it = this.getRawCentroids(
						batchID,
						groupID);
				while (it.hasNext()) {
					centroids.add(centroidFactory.create(it.next()));
				}
				return centroids;
			}
			finally {
				if (it != null) {
					try {
						it.close();
					}
					catch (final IOException e) {}
				}
			}

		}
		catch (final IOException e) {
			LOGGER.error("Cannot load centroids");
			throw new IOException(
					e);
		}

	}

	@Override
	@SuppressWarnings("unchecked")
	public AnalyticItemWrapper<T> getCentroid(
			final String id ) {
		return centroidFactory.create((T) dataStore.getEntry(
				index,
				new ByteArrayId(
						StringUtils.stringToBinary(id)),
				new ByteArrayId(
						StringUtils.stringToBinary(centroidDataTypeId))));
	}

	@SuppressWarnings("unchecked")
	protected CloseableIterator<T> getRawCentroids(
			final String batchId,
			final String groupID )
			throws IOException {

		final FilterFactoryImpl factory = new FilterFactoryImpl();
		final Expression expB1 = factory.property(ClusterFeatureAttribute.BATCH_ID.attrName());
		final Expression expB2 = factory.literal(batchId);

		final Filter batchIdFilter = factory.equal(
				expB1,
				expB2,
				false);

		Filter finalFilter = batchIdFilter;
		if (groupID != null) {
			final Expression exp1 = factory.property(ClusterFeatureAttribute.GROUP_ID.attrName());
			final Expression exp2 = factory.literal(groupID);
			// ignore levels for group IDS
			finalFilter = factory.and(
					factory.equal(
							exp1,
							exp2,
							false),
					batchIdFilter);
		}
		else if (level > 0) {
			final Expression exp1 = factory.property(ClusterFeatureAttribute.ZOOM_LEVEL.attrName());
			final Expression exp2 = factory.literal(level);
			finalFilter = factory.and(
					factory.equal(
							exp1,
							exp2,
							false),
					batchIdFilter);
		}

		final AccumuloCqlConstraintsQuery query = new AccumuloCqlConstraintsQuery(
				index,
				finalFilter,
				(FeatureDataAdapter) adapter,
				new String[] {});
		return (CloseableIterator<T>) query.query(
				basicAccumuloOperations,
				adapterStore,
				0);
	}

	@SuppressWarnings("unchecked")
	public void transferBatch(
			final String fromBatchId,
			final String groupID )
			throws IOException {
		final CloseableIterator<T> it = getRawCentroids(
				fromBatchId,
				groupID);
		int count = 0;
		final AccumuloDataStore store = new AccumuloDataStore(
				basicAccumuloOperations);
		while (it.hasNext()) {
			final AnalyticItemWrapper<T> item = centroidFactory.create(it.next());
			item.setBatchID(this.batchId);
			count++;
			store.ingest(
					(WritableDataAdapter<T>) adapter,
					index,
					item.getWrappedItem());
		}
		it.close();
		LOGGER.info("Transfer " + count + " centroids for " + fromBatchId + " to " + batchId);
	}

	@Override
	public int processForAllGroups(
			final CentroidProcessingFn<T> fn )
			throws IOException {
		List<String> centroidGroups;
		try {
			centroidGroups = getAllCentroidGroups();
		}
		catch (final IOException e) {
			throw new IOException(
					e);
		}

		int status = 0;
		for (final String groupID : centroidGroups) {
			status = fn.processGroup(
					groupID,
					getCentroidsForGroup(groupID));
			if (status != 0) {
				break;
			}
		}
		return status;
	}

	public static void fillOptions(
			final Set<Option> options ) {
		GlobalParameters.fillOptions(
				options,
				new GlobalParameters.Global[] {
					GlobalParameters.Global.ZOOKEEKER,
					GlobalParameters.Global.ACCUMULO_INSTANCE,
					GlobalParameters.Global.ACCUMULO_PASSWORD,
					GlobalParameters.Global.ACCUMULO_USER,
					GlobalParameters.Global.ACCUMULO_NAMESPACE,
					GlobalParameters.Global.BATCH_ID
				});

		CommonParameters.fillOptions(
				options,
				new CommonParameters.Common[] {
					CommonParameters.Common.ACCUMULO_CONNECT_FACTORY
				});

		CentroidParameters.fillOptions(
				options,
				new CentroidParameters.Centroid[] {
					CentroidParameters.Centroid.DATA_TYPE_ID,
					CentroidParameters.Centroid.DATA_NAMESPACE_URI,
					CentroidParameters.Centroid.INDEX_ID,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					CentroidParameters.Centroid.ZOOM_LEVEL
				});
	}

	public static void setParameters(
			final Configuration config,
			final String zookeeperUrl,
			final String instanceName,
			final String userName,
			final String password,
			final String tableNamespace,
			final Class<? extends AnalyticItemWrapperFactory> centroidFactory,
			final String centroidDataTypeId,
			final String indexId,
			final String batchId,
			final int level ) {
		RunnerUtils.setParameter(
				config,
				CentroidManagerGeoWave.class,
				new Object[] {
					centroidFactory,
					centroidDataTypeId,
					indexId,
					batchId,
					zookeeperUrl,
					instanceName,
					userName,
					password,
					tableNamespace,
					level
				},
				new ParameterEnum[] {
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					CentroidParameters.Centroid.DATA_TYPE_ID,
					CentroidParameters.Centroid.INDEX_ID,
					GlobalParameters.Global.BATCH_ID,
					GlobalParameters.Global.ZOOKEEKER,
					GlobalParameters.Global.ACCUMULO_INSTANCE,
					GlobalParameters.Global.ACCUMULO_USER,
					GlobalParameters.Global.ACCUMULO_PASSWORD,
					GlobalParameters.Global.ACCUMULO_NAMESPACE,
					CentroidParameters.Centroid.ZOOM_LEVEL,
				});
	}

	public static void setParameters(
			final Configuration config,
			final PropertyManagement runTimeProperties ) {
		RunnerUtils.setParameter(
				config,
				CentroidManagerGeoWave.class,
				runTimeProperties,
				new ParameterEnum[] {
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					CommonParameters.Common.ACCUMULO_CONNECT_FACTORY,
					CentroidParameters.Centroid.DATA_TYPE_ID,
					CentroidParameters.Centroid.DATA_NAMESPACE_URI,
					CentroidParameters.Centroid.INDEX_ID,
					GlobalParameters.Global.BATCH_ID,
					GlobalParameters.Global.ZOOKEEKER,
					GlobalParameters.Global.ACCUMULO_INSTANCE,
					GlobalParameters.Global.ACCUMULO_PASSWORD,
					GlobalParameters.Global.ACCUMULO_USER,
					CentroidParameters.Centroid.ZOOM_LEVEL,
					GlobalParameters.Global.ACCUMULO_NAMESPACE,
				});
	}

	@Override
	public ByteArrayId getDataTypeId() {
		return new ByteArrayId(
				StringUtils.stringToBinary(centroidDataTypeId));
	}

	@Override
	public ByteArrayId getIndexId() {
		return index.getId();
	}

}
