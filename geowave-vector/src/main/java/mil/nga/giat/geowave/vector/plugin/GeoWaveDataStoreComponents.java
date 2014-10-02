package mil.nga.giat.geowave.vector.plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.vector.VectorDataStore;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.vector.plugin.transaction.GeoWaveTransaction;
import mil.nga.giat.geowave.vector.query.row.TransformingVisibilityQuery;
import mil.nga.giat.geowave.vector.transaction.TransactionsAllocater;

import org.opengis.feature.simple.SimpleFeature;

public class GeoWaveDataStoreComponents
{
	private final FeatureDataAdapter adapter;
	private final VectorDataStore dataStore;
	private final VectorDataStore statsDataStore;
	private final GeoWaveGTDataStore GTstore;
	private final TransactionsAllocater transactionsAllocater;

	private Index currentIndex;

	public GeoWaveDataStoreComponents(
			final VectorDataStore dataStore,
			final VectorDataStore statsDataStore,
			final GeoWaveGTDataStore GTstore,
			final FeatureDataAdapter adapter,
			final TransactionsAllocater transactionsAllocater ) {
		this.adapter = adapter;
		this.dataStore = dataStore;
		this.statsDataStore = statsDataStore;
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

	public VectorDataStore getStatsDataStore() {
		return statsDataStore;
	}

	public GeoWaveGTDataStore getGTstore() {
		return GTstore;
	}

	public Index getCurrentIndex() {
		return currentIndex;
	}

	public void replaceFromVisibility(
			GeoWaveTransaction transaction,
			Collection<ByteArrayId> rowIDs,
			String expression,
			String replacement ) {
		this.dataStore.query(new TransformingVisibilityQuery(
				this.GTstore.getVisibilityManagement().visibilityReplacementIteratorClass(),
				currentIndex,
				rowIDs,
				transaction.composeAuthorizations(),
				expression,
				replacement));
	}

	public void remove(
			SimpleFeature feature,
			GeoWaveTransaction transaction )
			throws IOException {
		this.dataStore.deleteEntry(
				this.adapter,
				this.currentIndex,
				feature);
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
