package mil.nga.giat.geowave.analytics.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.analytics.clustering.CentroidManager.CentroidProcessingFn;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.CommonParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature.ClusterFeatureAttribute;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.dbops.BasicAccumuloOperationsBuilder;
import mil.nga.giat.geowave.analytics.tools.dbops.DirectBasicAccumuloOperationsBuilder;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.vector.query.AccumuloCqlConstraintsQuery;

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
import com.vividsolutions.jts.geom.Geometry;

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
 *                {@link BasicAccumuloOperationsBuilder}
 * 
 * @formatter:on
 * 
 * @param <T>
 *            The item type used to represent a centroid.
 */
public class NearestNeighborsQueryTool<T>
{
	final static Logger LOGGER = LoggerFactory.getLogger(NearestNeighborsQueryTool.class);

	private AccumuloOperations basicAccumuloOperations;

	private final AccumuloDataStore dataStore;
	private final AccumuloIndexStore indexStore;
	private final AccumuloAdapterStore adapterStore;
	private final Index index;

	public NearestNeighborsQueryTool(
			final BasicAccumuloOperations basicAccumuloOperations,
			final String indexId )
			throws AccumuloException,
			AccumuloSecurityException {
		this.basicAccumuloOperations = basicAccumuloOperations;
		dataStore = new AccumuloDataStore(
				basicAccumuloOperations);
		indexStore = new AccumuloIndexStore(
				basicAccumuloOperations);
		index = indexStore.getIndex(new ByteArrayId(
				StringUtils.stringToBinary(indexId)));
		adapterStore = new AccumuloAdapterStore(
				basicAccumuloOperations);
	}

	public NearestNeighborsQueryTool(
			final String zookeeperUrl,
			final String instanceName,
			final String userName,
			final String password,
			final String tableNamespace,
			final String indexId )
			throws AccumuloException,
			AccumuloSecurityException {
		this.basicAccumuloOperations = new BasicAccumuloOperations(
				zookeeperUrl,
				instanceName,
				userName,
				password,
				tableNamespace);
		dataStore = new AccumuloDataStore(
				basicAccumuloOperations);
		indexStore = new AccumuloIndexStore(
				basicAccumuloOperations);
		index = indexStore.getIndex(new ByteArrayId(
				StringUtils.stringToBinary(indexId)));
		adapterStore = new AccumuloAdapterStore(
				basicAccumuloOperations);

	}

	@SuppressWarnings("unchecked")
	public NearestNeighborsQueryTool(
			final ConfigurationWrapper context )
			throws AccumuloException,
			IOException,
			AccumuloSecurityException {

		
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
					BasicAccumuloOperationsBuilder.class,
					DirectBasicAccumuloOperationsBuilder.class).build(
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
	}

	@SuppressWarnings("unchecked")
	public NearestNeighborsQueryTool(
			final PropertyManagement runTimeProperties )
			throws AccumuloException,
			IOException,
			AccumuloSecurityException {
		final String indexId = runTimeProperties.getProperty(CentroidParameters.Centroid.INDEX_ID);

		basicAccumuloOperations = new BasicAccumuloOperations(
				runTimeProperties.getProperty(GlobalParameters.Global.ZOOKEEKER),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_INSTANCE),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_USER),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_PASSWORD),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_NAMESPACE));
		dataStore = new AccumuloDataStore(
				basicAccumuloOperations);
		indexStore = new AccumuloIndexStore(
				basicAccumuloOperations);
		index = indexStore.getIndex(new ByteArrayId(
				StringUtils.stringToBinary(indexId)));
		adapterStore = new AccumuloAdapterStore(
				basicAccumuloOperations);
	}

	
	@SuppressWarnings("unchecked")
	public CloseableIterator<T> getNeighbors(
			final ByteArrayId adapterId,
			final T point )
			throws IOException {
		@SuppressWarnings("rawtypes")
		DataAdapter<T> adapter = (DataAdapter<T>) this.adapterStore.getAdapter(	adapterId);
		final FilterFactoryImpl factory = new FilterFactoryImpl();
		
		final AccumuloCqlConstraintsQuery query = new AccumuloCqlConstraintsQuery(
				index,
				factory.bbox(getBoundingBox(point), bounds),
				(FeatureDataAdapter) adapter,
				new String[] {});
		return (CloseableIterator<T>) query.query(
				basicAccumuloOperations,
				adapterStore,
				0);
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
				NearestNeighborsQueryTool.class,
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
				NearestNeighborsQueryTool.class,
				runTimeProperties,
				new ParameterEnum[] {
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					CommonParameters.Common.ACCUMULO_CONNECT_FACTORY,
					CentroidParameters.Centroid.DATA_TYPE_ID,
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
		return new ByteArrayId(
				StringUtils.stringToBinary(indexId));
	}

}
