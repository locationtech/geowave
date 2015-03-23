package mil.nga.giat.geowave.analytics.clustering;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.analytics.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.CommonParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.dbops.BasicAccumuloOperationsFactory;
import mil.nga.giat.geowave.analytics.tools.dbops.DirectBasicAccumuloOperationsFactory;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.index.NumericIndexStrategyFactory.DataType;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.CustomIdIndex;
import mil.nga.giat.geowave.store.index.DimensionalityType;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.feature.type.BasicFeatureTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Polygon;

public class ClusteringUtils
{

	public static final String CLUSTERING_CRS = "EPSG:4326";

	final static Logger LOGGER = LoggerFactory.getLogger(ClusteringUtils.class);
	private static BasicAccumuloOperationsFactory ACCUMULO_OPERATIONS_FACTORY = new DirectBasicAccumuloOperationsFactory();

	public static void setBasicAccumuloOperationsBuilder(
			final BasicAccumuloOperationsFactory basicAccumuloOperationsBuilder ) {
		ClusteringUtils.ACCUMULO_OPERATIONS_FACTORY = basicAccumuloOperationsBuilder;
	}

	public static BasicAccumuloOperations createOperations(
			final String zookeeper,
			final String accumuloInstance,
			final String accumuloUser,
			final String accumuloPassword,
			final String namespace )
			throws Exception {

		return ACCUMULO_OPERATIONS_FACTORY.build(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				namespace);
	}

	public static Index createIndex(
			final String indexId,
			final String zookeeper,
			final String accumuloInstance,
			final String accumuloUser,
			final String accumuloPassword,
			final String namespace )
			throws Exception {

		final AccumuloOperations operations = new BasicAccumuloOperations(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				namespace);

		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);

		final ByteArrayId dbId = new ByteArrayId(
				indexId);
		if (!indexStore.indexExists(dbId)) {
			if (indexId.equals(IndexType.SPATIAL_VECTOR.getDefaultId())) {
				final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
				indexStore.addIndex(index);
				return index;
			}
			else if (indexId.equals(IndexType.SPATIAL_TEMPORAL_VECTOR.getDefaultId())) {
				final Index index = IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex();
				indexStore.addIndex(index);
				return index;
			}
			else {
				final Index index = new CustomIdIndex(
						IndexType.SPATIAL_VECTOR.createDefaultIndexStrategy(),
						IndexType.SPATIAL_VECTOR.getDefaultIndexModel(),
						DimensionalityType.SPATIAL,
						DataType.VECTOR,
						new ByteArrayId(
								indexId));
				indexStore.addIndex(index);
				return index;
			}
		}
		else {
			return indexStore.getIndex(dbId);
		}

	}

	public static DataAdapter<?> createAdapter(
			final String sampleDataTypeId,
			final String sampleDataNamespaceURI,
			final String zookeeper,
			final String accumuloInstance,
			final String accumuloUser,
			final String accumuloPassword,
			final String namespace,
			final String[] dimensionNames )
			throws Exception {

		final FeatureDataAdapter adapter = AnalyticFeature.createGeometryFeatureAdapter(
				sampleDataTypeId,
				dimensionNames,
				sampleDataNamespaceURI,
				CLUSTERING_CRS);

		final AccumuloOperations operations = ACCUMULO_OPERATIONS_FACTORY.build(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				namespace);

		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				operations);

		final ByteArrayId dbId = new ByteArrayId(
				sampleDataTypeId);
		if (!adapterStore.adapterExists(dbId)) {
			adapterStore.addAdapter(adapter);
			return adapter;
		}
		else {
			return adapterStore.getAdapter(dbId);
		}

	}

	public static DataAdapter[] getAdapters(
			final String zookeeper,
			final String accumuloInstance,
			final String accumuloUser,
			final String accumuloPassword,
			final String namespace )
			throws IOException {
		BasicAccumuloOperations ops;
		try {
			ops = new BasicAccumuloOperations(
					zookeeper,
					accumuloInstance,
					accumuloUser,
					accumuloPassword,
					namespace);

			final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
					ops);
			final mil.nga.giat.geowave.store.CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
			final List<DataAdapter> adapters = new LinkedList<DataAdapter>();
			while (it.hasNext()) {
				adapters.add(it.next());
			}

			final DataAdapter[] result = new DataAdapter[adapters.size()];
			adapters.toArray(result);
			return result;
		}
		catch (final AccumuloException e) {
			throw new IOException(
					"Cannot connect to GeoWave for Adapter Inquiry (" + accumuloInstance + "@ " + zookeeper + ")");
		}
		catch (final AccumuloSecurityException e) {
			throw new IOException(
					"Cannot connect to GeoWave for Adapter Inquiry (" + accumuloInstance + "@ " + zookeeper + ")");
		}
	}

	public static Index[] getIndices(
			final String zookeeper,
			final String accumuloInstance,
			final String accumuloUser,
			final String accumuloPassword,
			final String namespace ) {
		BasicAccumuloOperations ops;
		try {
			ops = ACCUMULO_OPERATIONS_FACTORY.build(
					zookeeper,
					accumuloInstance,
					accumuloUser,
					accumuloPassword,
					namespace);

			final AccumuloIndexStore indexStore = new AccumuloIndexStore(
					ops);
			final mil.nga.giat.geowave.store.CloseableIterator<Index> it = indexStore.getIndices();
			final List<Index> indices = new LinkedList<Index>();
			while (it.hasNext()) {
				indices.add(it.next());
			}

			final Index[] result = new Index[indices.size()];
			indices.toArray(result);
			return result;
		}
		catch (final AccumuloException e) {
			LOGGER.error(
					"Cannot connect to GeoWave for Index Inquiry (" + accumuloInstance + "@ " + zookeeper + ")",
					e);
		}
		catch (final AccumuloSecurityException e) {
			LOGGER.error(
					"Cannot connect to GeoWave for Index Inquiry (" + accumuloInstance + "@ " + zookeeper + ")",
					e);
		}
		return new Index[] {
			IndexType.SPATIAL_VECTOR.createDefaultIndex()
		};
	}

	/*
	 * Method takes in a polygon and generates the corresponding ranges in a
	 * GeoWave spatial index
	 */
	protected static List<ByteArrayRange> getGeoWaveRangesForQuery(
			final Polygon polygon ) {

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final List<ByteArrayRange> ranges = index.getIndexStrategy().getQueryRanges(
				new SpatialQuery(
						polygon).getIndexConstraints(index.getIndexStrategy()));

		return ranges;
	}

	public static Index createIndex(
			final PropertyManagement propertyManagement )
			throws Exception {
		return ClusteringUtils.createIndex(
				propertyManagement.getProperty(CentroidParameters.Centroid.INDEX_ID),
				propertyManagement.getProperty(GlobalParameters.Global.ZOOKEEKER),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_INSTANCE),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_USER),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_PASSWORD),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_NAMESPACE));

	}

	public static BasicAccumuloOperations createOperations(
			final PropertyManagement propertyManagement )
			throws Exception {
		return ClusteringUtils.createOperations(
				propertyManagement.getProperty(GlobalParameters.Global.ZOOKEEKER),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_INSTANCE),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_USER),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_PASSWORD),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_NAMESPACE));

	}

	public static DataAdapter<?> createAdapter(
			final PropertyManagement propertyManagement )
			throws Exception {

		Class<DimensionExtractor> dimensionExtractorClass = propertyManagement.getPropertyAsClass(
				CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
				DimensionExtractor.class);

		return ClusteringUtils.createAdapter(
				propertyManagement.getProperty(CentroidParameters.Centroid.DATA_TYPE_ID),
				propertyManagement.getProperty(
						CentroidParameters.Centroid.DATA_NAMESPACE_URI,
						BasicFeatureTypes.DEFAULT_NAMESPACE),
				propertyManagement.getProperty(GlobalParameters.Global.ZOOKEEKER),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_INSTANCE),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_USER),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_PASSWORD),
				propertyManagement.getProperty(GlobalParameters.Global.ACCUMULO_NAMESPACE),
				dimensionExtractorClass.newInstance().getDimensionNames());
	}
}
