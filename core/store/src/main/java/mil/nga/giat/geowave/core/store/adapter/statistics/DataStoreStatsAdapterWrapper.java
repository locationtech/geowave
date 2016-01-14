package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.util.Arrays;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/*
 * The name is bit amusing. 
 * This class is used by the DataStores to add additional data store specific statistics such as row ranges and histograms.
 * The class wraps an adapter, overriding the methods {@link DataStoreStatsAdapterWrapper#getSupportedStatisticsIds} and 
 *   {@link DataStoreStatsAdapterWrapper#createDataStatistics}.
 * 
 */
public class DataStoreStatsAdapterWrapper<T> implements
		StatisticalDataAdapter<T>
{

	private WritableDataAdapter<T> adapter;
	final PrimaryIndex index;

	public DataStoreStatsAdapterWrapper(
			final PrimaryIndex index,
			final WritableDataAdapter<T> adapter ) {
		super();
		this.index = index;
		this.adapter = adapter;
	}

	public void setAdapter(
			WritableDataAdapter<T> adapter ) {
		this.adapter = adapter;
	}

	@Override
	public ByteArrayId getAdapterId() {
		return adapter.getAdapterId();
	}

	@Override
	public boolean isSupported(
			final T entry ) {
		return adapter.isSupported(entry);
	}

	@Override
	public ByteArrayId getDataId(
			final T entry ) {
		return adapter.getDataId(entry);
	}

	@Override
	public T decode(
			IndexedAdapterPersistenceEncoding data,
			PrimaryIndex index ) {
		return adapter.decode(
				data,
				index);
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final T entry,
			final CommonIndexModel indexModel ) {
		return adapter.encode(
				entry,
				indexModel);
	}

	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		return adapter.getReader(fieldId);
	}

	@Override
	public byte[] toBinary() {
		return adapter.toBinary();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		adapter.fromBinary(bytes);
	}

	@Override
	public FieldWriter<T, Object> getWriter(
			final ByteArrayId fieldId ) {
		return adapter.getWriter(fieldId);
	}

	@Override
	public ByteArrayId[] getSupportedStatisticsIds() {
		final ByteArrayId[] idsFromAdapter = (adapter instanceof StatisticalDataAdapter) ? ((StatisticalDataAdapter) adapter).getSupportedStatisticsIds() : new ByteArrayId[0];
		final ByteArrayId[] newSet = Arrays.copyOf(
				idsFromAdapter,
				idsFromAdapter.length + 2);
		newSet[idsFromAdapter.length] = RowRangeDataStatistics.STATS_ID;
		newSet[idsFromAdapter.length + 1] = RowRangeHistogramStatistics.STATS_ID;
		return newSet;
	}

	@Override
	public DataStatistics<T> createDataStatistics(
			final ByteArrayId statisticsId ) {
		if (statisticsId.equals(RowRangeDataStatistics.STATS_ID)) {
			return new RowRangeDataStatistics(
					index.getId());
		}
		if (statisticsId.equals(RowRangeHistogramStatistics.STATS_ID)) {
			return new RowRangeHistogramStatistics(
					adapter.getAdapterId(),
					index.getId(),
					1024);
		}
		return (adapter instanceof StatisticalDataAdapter) ? ((StatisticalDataAdapter) adapter).createDataStatistics(statisticsId) : null;
	}

	@Override
	public EntryVisibilityHandler<T> getVisibilityHandler(
			final ByteArrayId statisticsId ) {
		return (adapter instanceof StatisticalDataAdapter) ? ((StatisticalDataAdapter) adapter).getVisibilityHandler(statisticsId) : new EmptyStatisticVisibility<T>();
	}

	public DataAdapter<T> getAdapter() {
		return adapter;
	}

}
