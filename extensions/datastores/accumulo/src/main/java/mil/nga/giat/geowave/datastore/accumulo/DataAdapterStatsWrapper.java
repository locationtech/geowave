package mil.nga.giat.geowave.datastore.accumulo;

import java.util.Arrays;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.statistics.EmptyStatisticVisibility;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;

public class DataAdapterStatsWrapper<T> implements
		StatisticalDataAdapter<T>
{

	final DataAdapter<T> adapter;
	final Index index;

	public DataAdapterStatsWrapper(
			final Index index,
			DataAdapter<T> adapter ) {
		super();
		this.index = index;
		this.adapter = adapter;
	}

	@Override
	public ByteArrayId getAdapterId() {
		return adapter.getAdapterId();
	}

	@Override
	public boolean isSupported(
			T entry ) {
		return adapter.isSupported(entry);
	}

	@Override
	public ByteArrayId getDataId(
			T entry ) {
		return adapter.getDataId(entry);
	}

	@Override
	public T decode(
			IndexedAdapterPersistenceEncoding data,
			Index index ) {
		return adapter.decode(
				data,
				index);
	}

	@Override
	public AdapterPersistenceEncoding encode(
			T entry,
			CommonIndexModel indexModel ) {
		return adapter.encode(
				entry,
				indexModel);
	}

	@Override
	public FieldReader<Object> getReader(
			ByteArrayId fieldId ) {
		return adapter.getReader(fieldId);
	}

	@Override
	public byte[] toBinary() {
		return adapter.toBinary();
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		adapter.fromBinary(bytes);
	}

	@Override
	public FieldWriter<T, Object> getWriter(
			ByteArrayId fieldId ) {
		return (adapter instanceof WritableDataAdapter) ? ((WritableDataAdapter<T>) adapter).getWriter(fieldId) : null;
	}

	@Override
	public ByteArrayId[] getSupportedStatisticsIds() {
		final ByteArrayId[] idsFromAdapter = (adapter instanceof StatisticalDataAdapter) ? ((StatisticalDataAdapter) adapter).getSupportedStatisticsIds() : new ByteArrayId[0];
		final ByteArrayId[] newSet = Arrays.copyOf(
				idsFromAdapter,
				idsFromAdapter.length + 1);
		newSet[idsFromAdapter.length] = RowRangeDataStatistics.STATS_ID;
		return newSet;
	}

	@Override
	public DataStatistics<T> createDataStatistics(
			ByteArrayId statisticsId ) {
		if (statisticsId.equals(RowRangeDataStatistics.STATS_ID)) return new RowRangeDataStatistics(
				index.getId());
		return (adapter instanceof StatisticalDataAdapter) ? ((StatisticalDataAdapter) adapter).createDataStatistics(statisticsId) : null;
	}

	@Override
	public DataStatisticsVisibilityHandler<T> getVisibilityHandler(
			ByteArrayId statisticsId ) {
		return (adapter instanceof StatisticalDataAdapter) ? ((StatisticalDataAdapter) adapter).getVisibilityHandler(statisticsId) : new EmptyStatisticVisibility<T>();
	}

	public DataAdapter<T> getAdapter() {
		return adapter;
	}

}
