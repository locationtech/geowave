package mil.nga.giat.geowave.core.store.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.NativeGeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.NativeGeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class MergingEntryIterator<T> extends
		NativeEntryIteratorWrapper<T>
{
	private final static Logger LOGGER = Logger.getLogger(
			NativeEntryIteratorWrapper.class);

	private final Map<ByteArrayId, RowMergingDataAdapter> mergingAdapters;
	private final Map<ByteArrayId, RowTransform> transforms;

	private NativeGeoWaveRow peekedValue;

	public MergingEntryIterator(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<NativeGeoWaveRow> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T> scanCallback,
			final Map<ByteArrayId, RowMergingDataAdapter> mergingAdapters ) {
		super(
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback,
				true);
		this.mergingAdapters = mergingAdapters;
		transforms = new HashMap<ByteArrayId, RowTransform>();
	}

	@Override
	protected Object getNextEncodedResult() {

		// Get next result from scanner
		// We may have already peeked at it
		NativeGeoWaveRow nextResult = null;
		if (peekedValue != null) {
			nextResult = peekedValue;
		}
		else {
			nextResult = (NativeGeoWaveRow) scannerIt.next();
		}
		peekedValue = null;

		final ByteArrayId adapterId = new ByteArrayId(
				nextResult.getAdapterId());
		final RowMergingDataAdapter mergingAdapter = mergingAdapters.get(
				adapterId);

		final ArrayList<NativeGeoWaveRow> resultsToMerge = new ArrayList<NativeGeoWaveRow>();

		if ((mergingAdapter != null) && (mergingAdapter.getTransform() != null)) {

			resultsToMerge.add(
					nextResult);

			// Peek ahead to see if it needs to be merged with the next result
			while (scannerIt.hasNext()) {
				peekedValue = (NativeGeoWaveRow) scannerIt.next();

				if (DataStoreUtils.rowIdsMatch(
						nextResult,
						peekedValue)) {

					resultsToMerge.add(
							peekedValue);
					peekedValue = null;
				}
				else {
					if (resultsToMerge.size() > 1) {
						nextResult = mergeResults(
								mergingAdapter,
								resultsToMerge);
					}
					resultsToMerge.clear();
					return nextResult;
				}
			}
			// If the last results in the scanner are mergeable, merge them
			if (resultsToMerge.size() > 1) {
				nextResult = mergeResults(
						mergingAdapter,
						resultsToMerge);
			}
			resultsToMerge.clear();
		}

		return nextResult;
	}

	private NativeGeoWaveRow mergeResults(
			final RowMergingDataAdapter mergingAdapter,
			final ArrayList<NativeGeoWaveRow> resultsToMerge ) {

		Collections.sort(
				resultsToMerge,
				new Comparator<NativeGeoWaveRow>() {
					@Override
					public int compare(
							final NativeGeoWaveRow row1,
							final NativeGeoWaveRow row2 ) {

						final ByteBuffer buf1 = ByteBuffer.wrap(
								row1.getDataId());
						final ByteBuffer buf2 = ByteBuffer.wrap(
								row2.getDataId());
						buf1.get();
						buf2.get();

						final long ts1 = buf1.getLong();
						final long ts2 = buf2.getLong();

						return Long.compare(
								ts2,
								ts1);
					}

				});

		final Iterator<NativeGeoWaveRow> iter = resultsToMerge.iterator();
		NativeGeoWaveRow mergedResult = iter.next();
		while (iter.hasNext()) {
			mergedResult = merge(
					mergingAdapter,
					mergedResult,
					iter.next());
		}

		return mergedResult;
	}

	private NativeGeoWaveRow merge(
			final RowMergingDataAdapter mergingAdapter,
			final NativeGeoWaveRow row,
			final NativeGeoWaveRow rowToMerge ) {

		RowTransform transform = transforms.get(
				mergingAdapter.getAdapterId());
		if (transform == null) {
			transform = mergingAdapter.getTransform();
			// set strategy
			try {
				transform.initOptions(
						mergingAdapter.getOptions(
								null));
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to initialize merge strategy for adapter: " + mergingAdapter.getAdapterId(),
						e);
			}
			transforms.put(
					mergingAdapter.getAdapterId(),
					transform);
		}

		final Mergeable mergeable = transform.getRowAsMergeableObject(
				new ByteArrayId(
						row.getAdapterId()),
				new ByteArrayId(
						row.getFieldMask()),
				row.getValue());

		mergeable.merge(
				transform.getRowAsMergeableObject(
						new ByteArrayId(
								rowToMerge.getAdapterId()),
						new ByteArrayId(
								rowToMerge.getFieldMask()),
						rowToMerge.getValue()));

		return new NativeGeoWaveRowImpl(
				DataStoreUtils.removeUniqueId(
						row.getDataId()),
				row.getAdapterId(),
				row.getIndex(),
				row.getFieldMask(),
				transform.getBinaryFromMergedObject(
						mergeable));

	}

	@Override
	protected boolean hasNextScannedResult() {
		return (peekedValue != null) || scannerIt.hasNext();
	}

	@Override
	public void remove() {
		throw new NotImplementedException(
				"Transforming iterator cannot use remove()");
	}

}
