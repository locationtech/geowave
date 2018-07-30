package mil.nga.giat.geowave.core.store.util;

import java.util.Iterator;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStoreUtils;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class NativeEntryTransformer<T> implements
		GeoWaveRowIteratorTransformer<T>
{
	private final AdapterStore adapterStore;
	private final PrimaryIndex index;
	private final QueryFilter clientFilter;
	private final ScanCallback<T, ? extends GeoWaveRow> scanCallback;
	private final byte[] fieldSubsetBitmask;
	private final double[] maxResolutionSubsamplingPerDimension;
	private final boolean decodePersistenceEncoding;

	public NativeEntryTransformer(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final QueryFilter clientFilter,
			final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
			final byte[] fieldSubsetBitmask,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean decodePersistenceEncoding ) {
		this.adapterStore = adapterStore;
		this.index = index;
		this.clientFilter = clientFilter;
		this.scanCallback = scanCallback;
		this.fieldSubsetBitmask = fieldSubsetBitmask;
		this.decodePersistenceEncoding = decodePersistenceEncoding;
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
	}

	@Override
	public Iterator<T> apply(
			Iterator<GeoWaveRow> rowIter ) {
		return new NativeEntryIteratorWrapper<T>(
				adapterStore,
				index,
				rowIter,
				clientFilter,
				scanCallback,
				fieldSubsetBitmask,
				maxResolutionSubsamplingPerDimension,
				decodePersistenceEncoding);
	}
}
