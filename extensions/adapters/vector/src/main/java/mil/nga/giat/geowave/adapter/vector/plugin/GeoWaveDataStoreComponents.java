package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.opengis.feature.simple.SimpleFeature;

public class GeoWaveDataStoreComponents
{
	private final GeotoolsFeatureDataAdapter adapter;
	private final DataStore dataStore;
	private final IndexStore indexStore;
	private final DataStatisticsStore dataStatisticsStore;
	private final GeoWaveGTDataStore gtStore;
	private final TransactionsAllocator transactionAllocator;

	private final List<PrimaryIndex> writeIndices;

	public GeoWaveDataStoreComponents(
			final DataStore dataStore,
			final DataStatisticsStore dataStatisticsStore,
			final IndexStore indexStore,
			final GeotoolsFeatureDataAdapter adapter,
			final GeoWaveGTDataStore gtStore,
			final TransactionsAllocator transactionAllocator ) {
		this.adapter = adapter;
		this.dataStore = dataStore;
		this.indexStore = indexStore;
		this.dataStatisticsStore = dataStatisticsStore;
		this.gtStore = gtStore;
		writeIndices = gtStore.getWriteIndices(adapter);
		this.transactionAllocator = transactionAllocator;
	}

	public IndexStore getIndexStore() {
		return indexStore;
	}

	public GeotoolsFeatureDataAdapter getAdapter() {
		return adapter;
	}

	public DataStore getDataStore() {
		return dataStore;
	}

	public GeoWaveGTDataStore getGTstore() {
		return gtStore;
	}

	public List<PrimaryIndex> getWriteIndices() {
		return writeIndices;
	}

	public DataStatisticsStore getStatsStore() {
		return dataStatisticsStore;
	}

	public CloseableIterator<Index<?, ?>> getIndices(
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
			final BasicQuery query ) {
		return getGTstore().getIndexQueryStrategy().getIndices(
				stats,
				query,
				getIndexStore().getIndices());
	}

	public void remove(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {

		final QueryOptions options = new QueryOptions(
				adapter);
		options.setAuthorizations(transaction.composeAuthorizations());
		options.setIndices(writeIndices.toArray(new Index[writeIndices.size()]));

		dataStore.delete(
				options,
				new DataIdQuery(
						adapter.getAdapterId(),
						adapter.getDataId(feature)));
	}

	public void remove(
			final String fid,
			final GeoWaveTransaction transaction )
			throws IOException {

		final QueryOptions options = new QueryOptions(
				adapter);
		options.setAuthorizations(transaction.composeAuthorizations());
		options.setIndices(writeIndices.toArray(new Index[writeIndices.size()]));

		dataStore.delete(
				options,
				new DataIdQuery(
						new ByteArrayId(
								StringUtils.stringToBinary(fid)),
						adapter.getAdapterId()));

	}

	@SuppressWarnings("unchecked")
	public void write(
			final Iterator<SimpleFeature> featureIt,
			final Set<String> fidList,
			final GeoWaveTransaction transaction )
			throws IOException {
		for (PrimaryIndex currentIndex : this.writeIndices) {
			try (IndexWriter indexWriter = dataStore.createIndexWriter(
					currentIndex,
					new UniformVisibilityWriter<SimpleFeature>(
							new GlobalVisibilityHandler(
									transaction.composeVisibility())))) {
				while (featureIt.hasNext()) {
					final SimpleFeature feature = featureIt.next();
					fidList.add(feature.getID());
					indexWriter.write(
							adapter,
							feature);
				}
			}
		}
	}

	public void writeCommit(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {
		for (PrimaryIndex currentIndex : this.writeIndices) {
			try (IndexWriter indexWriter = dataStore.createIndexWriter(
					currentIndex,
					new UniformVisibilityWriter<SimpleFeature>(
							new GlobalVisibilityHandler(
									transaction.composeVisibility())))) {
				indexWriter.write(
						adapter,
						feature);
			}
		}
	}

	public String getTransaction()
			throws IOException {
		return transactionAllocator.getTransaction();
	}

	public void releaseTransaction(
			final String txID )
			throws IOException {
		transactionAllocator.releaseTransaction(txID);
	}
}
