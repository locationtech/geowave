package mil.nga.giat.geowave.analytic.clustering;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.StoreParameters;
import mil.nga.giat.geowave.analytic.store.PersistableAdapterStore;
import mil.nga.giat.geowave.analytic.store.PersistableIndexStore;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;

import org.geotools.feature.type.BasicFeatureTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Polygon;

public class ClusteringUtils
{

	public static final String CLUSTERING_CRS = "EPSG:4326";

	final static Logger LOGGER = LoggerFactory.getLogger(ClusteringUtils.class);

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
			final PropertyManagement propertyManagement )
			throws IOException {
		final AdapterStore adapterStore = ((PersistableAdapterStore) StoreParameters.StoreParam.ADAPTER_STORE.getHelper().getValue(
				propertyManagement)).getCliOptions().createStore();

		final mil.nga.giat.geowave.core.store.CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
		final List<DataAdapter> adapters = new LinkedList<DataAdapter>();
		while (it.hasNext()) {
			adapters.add(it.next());
		}
		it.close();
		final DataAdapter[] result = new DataAdapter[adapters.size()];
		adapters.toArray(result);
		return result;
	}

	public static PrimaryIndex[] getIndices(
			final PropertyManagement propertyManagement ) {

		final IndexStore indexStore = ((PersistableIndexStore) StoreParameters.StoreParam.INDEX_STORE.getHelper().getValue(
				propertyManagement)).getCliOptions().createStore();

		final mil.nga.giat.geowave.core.store.CloseableIterator<Index<?, ?>> it = indexStore.getIndices();
		final List<PrimaryIndex> indices = new LinkedList<PrimaryIndex>();
		while (it.hasNext()) {
			indices.add((PrimaryIndex) it.next());
		}
		try {
			it.close();
		}
		catch (final IOException e) {
			LOGGER.warn("Unable to close iterator" + e);
		}
		final PrimaryIndex[] result = new PrimaryIndex[indices.size()];
		indices.toArray(result);
		return result;
	}

	/*
	 * Method takes in a polygon and generates the corresponding ranges in a
	 * GeoWave spatial index
	 */
	protected static List<ByteArrayRange> getGeoWaveRangesForQuery(
			final Polygon polygon ) {

		final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex();
		final List<ByteArrayRange> ranges = DataStoreUtils.constraintsToByteArrayRanges(
				new SpatialQuery(
						polygon).getIndexConstraints(index.getIndexStrategy()),
				index.getIndexStrategy(),
				-1);

		return ranges;
	}

	public static PrimaryIndex createIndex(
			final PropertyManagement propertyManagement )
			throws Exception {

		final IndexStore indexStore = ((PersistableIndexStore) StoreParameters.StoreParam.INDEX_STORE.getHelper().getValue(
				propertyManagement)).getCliOptions().createStore();
		return (PrimaryIndex) indexStore.getIndex(new ByteArrayId(
				propertyManagement.getPropertyAsString(CentroidParameters.Centroid.INDEX_ID)));
	}

	public static DataAdapter<?> createAdapter(
			final PropertyManagement propertyManagement )
			throws Exception {

		final Class<DimensionExtractor> dimensionExtractorClass = propertyManagement.getPropertyAsClass(
				CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
				DimensionExtractor.class);

		return ClusteringUtils.createAdapter(
				propertyManagement.getPropertyAsString(CentroidParameters.Centroid.DATA_TYPE_ID),
				propertyManagement.getPropertyAsString(
						CentroidParameters.Centroid.DATA_NAMESPACE_URI,
						BasicFeatureTypes.DEFAULT_NAMESPACE),
				((PersistableAdapterStore) StoreParameters.StoreParam.ADAPTER_STORE.getHelper().getValue(
						propertyManagement)).getCliOptions().createStore(),
				dimensionExtractorClass.newInstance().getDimensionNames());
	}
}
