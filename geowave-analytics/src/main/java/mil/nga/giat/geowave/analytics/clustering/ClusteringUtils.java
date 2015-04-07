package mil.nga.giat.geowave.analytics.clustering;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.analytics.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.CommonParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.dbops.AccumuloAdapterStoreFactory;
import mil.nga.giat.geowave.analytics.tools.dbops.DirectBasicAccumuloOperationsFactory;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.index.NumericIndexStrategyFactory.DataType;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
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

	private static Index createIndex(
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

	private static DataAdapter<?> createAdapter(
			final String sampleDataTypeId,
			final String sampleDataNamespaceURI,
			final AdapterStore adapterStore,
			final String[] dimensionNames )
			throws Exception {

		final FeatureDataAdapter adapter = AnalyticFeature.createGeometryFeatureAdapter(
				sampleDataTypeId,
				dimensionNames,
				sampleDataNamespaceURI,
				CLUSTERING_CRS);

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
			PropertyManagement propertyManagement )
			throws IOException {
		BasicAccumuloOperations ops;
		try {

			final AdapterStore adapterStore = propertyManagement.getClassInstance(
					CommonParameters.Common.ADAPTER_STORE_FACTORY,
					AccumuloAdapterStoreFactory.class).getAdapterStore(
					propertyManagement);

			final mil.nga.giat.geowave.store.CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
			final List<DataAdapter> adapters = new LinkedList<DataAdapter>();
			while (it.hasNext()) {
				adapters.add(it.next());
			}

			final DataAdapter[] result = new DataAdapter[adapters.size()];
			adapters.toArray(result);
			return result;
		}
		catch (InstantiationException e) {
			throw new IOException(
					e);
		}
	}

	public static Index[] getIndices(
			PropertyManagement propertyManagement ) {
		BasicAccumuloOperations ops;
		final String zookeeper = propertyManagement.getPropertyAsString(
				GlobalParameters.Global.ZOOKEEKER,
				"localhost:2181");
		final String accumuloInstance = propertyManagement.getPropertyAsString(
				GlobalParameters.Global.ACCUMULO_INSTANCE,
				"miniInstance");

		try {
			ops = propertyManagement.getClassInstance(
					CommonParameters.Common.ACCUMULO_CONNECT_FACTORY,
					DirectBasicAccumuloOperationsFactory.class).build(
					zookeeper,
					accumuloInstance,
					propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_USER),
					propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_PASSWORD),
					propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_NAMESPACE));

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
		catch (InstantiationException e) {
			LOGGER.error(
					"cannot connect to GeoWave ",
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
				propertyManagement.getPropertyAsString(CentroidParameters.Centroid.INDEX_ID),
				propertyManagement.getPropertyAsString(GlobalParameters.Global.ZOOKEEKER),
				propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_INSTANCE),
				propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_USER),
				propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_PASSWORD),
				propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_NAMESPACE));

	}

	public static BasicAccumuloOperations createOperations(
			final PropertyManagement propertyManagement )
			throws Exception {
		return propertyManagement.getClassInstance(
				CommonParameters.Common.ACCUMULO_CONNECT_FACTORY,
				DirectBasicAccumuloOperationsFactory.class).build(
				propertyManagement.getPropertyAsString(GlobalParameters.Global.ZOOKEEKER),
				propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_INSTANCE),
				propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_USER),
				propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_PASSWORD),
				propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_NAMESPACE));

	}

	public static DataAdapter<?> createAdapter(
			final PropertyManagement propertyManagement )
			throws Exception {

		Class<DimensionExtractor> dimensionExtractorClass = propertyManagement.getPropertyAsClass(
				CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
				DimensionExtractor.class);

		return ClusteringUtils.createAdapter(
				propertyManagement.getPropertyAsString(CentroidParameters.Centroid.DATA_TYPE_ID),
				propertyManagement.getPropertyAsString(
						CentroidParameters.Centroid.DATA_NAMESPACE_URI,
						BasicFeatureTypes.DEFAULT_NAMESPACE),
				propertyManagement.getClassInstance(
						CommonParameters.Common.ADAPTER_STORE_FACTORY,
						AccumuloAdapterStoreFactory.class).getAdapterStore(
						propertyManagement),
				dimensionExtractorClass.newInstance().getDimensionNames());
	}
}
