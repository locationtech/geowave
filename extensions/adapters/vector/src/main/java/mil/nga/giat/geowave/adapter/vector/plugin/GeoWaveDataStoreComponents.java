package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.AccumuloDataStatisticsStoreExt;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.VectorDataStore;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import mil.nga.giat.geowave.adapter.vector.query.TransformingVisibilityQuery;
import mil.nga.giat.geowave.adapter.vector.transaction.TransactionsAllocater;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.util.VisibilityTransformer;

import org.opengis.feature.simple.SimpleFeature;

public class GeoWaveDataStoreComponents
{
	private final FeatureDataAdapter adapter;
	private final VectorDataStore dataStore;
	private final GeoWaveGTDataStore GTstore;
	private final TransactionsAllocater transactionsAllocater;

	private Index currentIndex;

	public GeoWaveDataStoreComponents(
			final VectorDataStore dataStore,
			final GeoWaveGTDataStore GTstore,
			final FeatureDataAdapter adapter,
			final TransactionsAllocater transactionsAllocater ) {
		this.adapter = adapter;
		this.dataStore = dataStore;
		this.GTstore = GTstore;
		this.currentIndex = GTstore.getIndex(adapter);
		this.transactionsAllocater = transactionsAllocater;
	}

	public FeatureDataAdapter getAdapter() {
		return adapter;
	}

	public VectorDataStore getDataStore() {
		return dataStore;
	}

	public GeoWaveGTDataStore getGTstore() {
		return GTstore;
	}

	public Index getCurrentIndex() {
		return currentIndex;
	}

	public void replaceDataVisibility(
			GeoWaveTransaction transaction,
			Collection<ByteArrayId> rowIDs,
			VisibilityTransformer transformer ) {
		dataStore.query(new TransformingVisibilityQuery(
				transformer,
				currentIndex,
				rowIDs,
				transaction.composeAuthorizations()));
	}

	public void replaceStatsVisibility(
			GeoWaveTransaction transaction,
			VisibilityTransformer transformer ) {
		((AccumuloDataStatisticsStoreExt) dataStore.getStatsStore()).transformVisibility(
				this.adapter.getAdapterId(),
				transformer,
				transaction.composeAuthorizations());
	}

	@SuppressWarnings("unchecked")
	public Map<ByteArrayId, DataStatistics<SimpleFeature>> getDataStatistics(
			GeoWaveTransaction transaction ) {
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();

		for (ByteArrayId statsId : adapter.getSupportedStatisticsIds()) {
			@SuppressWarnings("unused")
			DataStatistics<SimpleFeature> put = stats.put(
					statsId,
					(DataStatistics<SimpleFeature>) dataStore.getStatsStore().getDataStatistics(
							this.adapter.getAdapterId(),
							statsId,
							transaction.composeAuthorizations()));
		}
		return stats;
	}

	public void remove(
			SimpleFeature feature,
			GeoWaveTransaction transaction )
			throws IOException {

		this.dataStore.deleteEntry(
				this.currentIndex,
				adapter.getDataId(feature),
				adapter.getAdapterId(),
				transaction.composeAuthorizations());
	}

	public void remove(
			String fid,
			GeoWaveTransaction transaction )
			throws IOException {

		this.dataStore.deleteEntry(
				this.currentIndex,
				new ByteArrayId(
						StringUtils.stringToBinary(fid)),
				adapter.getAdapterId(),
				transaction.composeAuthorizations());
	}

	@SuppressWarnings("unchecked")
	public List<ByteArrayId> write(
			SimpleFeature feature,
			GeoWaveTransaction transaction )
			throws IOException {
		return this.dataStore.ingest(
				this.adapter,
				this.currentIndex,
				feature,
				new UniformVisibilityWriter<SimpleFeature>(
						new GlobalVisibilityHandler(
								transaction.composeVisibility())));
	}

	@SuppressWarnings("unchecked")
	public void write(
			final Iterator<SimpleFeature> featureIt,
			final Map<String, List<ByteArrayId>> fidList,
			final GeoWaveTransaction transaction )
			throws IOException {
		this.dataStore.ingest(
				this.adapter,
				this.currentIndex,
				featureIt,
				new IngestCallback<SimpleFeature>() {

					@Override
					public void entryIngested(
							DataStoreEntryInfo entryInfo,
							SimpleFeature entry ) {
						fidList.put(
								entry.getID(),
								entryInfo.getRowIds());
					}

				},
				new UniformVisibilityWriter<SimpleFeature>(
						new GlobalVisibilityHandler(
								transaction.composeVisibility())));
	}

	public List<ByteArrayId> writeCommit(
			SimpleFeature feature,
			GeoWaveTransaction transaction )
			throws IOException {
		return this.dataStore.ingest(
				this.adapter,
				this.currentIndex,
				feature);
	}

	public String getTransaction()
			throws IOException {
		return transactionsAllocater.getTransaction();
	}

	public void releaseTransaction(
			String txID )
			throws IOException {
		transactionsAllocater.releaseTransaction(txID);
	}
}
