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
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class MergingEntryIterator<T> extends
		NativeEntryIteratorWrapper<T>
{
	private final static Logger LOGGER = Logger.getLogger(NativeEntryIteratorWrapper.class);

	private final Map<ByteArrayId, RowMergingDataAdapter> mergingAdapters;
	private final Map<ByteArrayId, RowTransform> transforms;

	private GeoWaveRow peekedValue;

	public MergingEntryIterator(
			final BaseDataStore dataStore,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<GeoWaveRow> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T, GeoWaveRow> scanCallback,
			final Map<ByteArrayId, RowMergingDataAdapter> mergingAdapters ) {
		super(
				dataStore,
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
	protected GeoWaveRow getNextEncodedResult() {

		// Get next result from scanner
		// We may have already peeked at it
		GeoWaveRow nextResult = null;
		if (peekedValue != null) {
			nextResult = peekedValue;
		}
		else {
			nextResult = scannerIt.next();
		}
		peekedValue = null;

		final ByteArrayId adapterId = new ByteArrayId(
				nextResult.getAdapterId());
		final RowMergingDataAdapter mergingAdapter = mergingAdapters.get(adapterId);

		final ArrayList<GeoWaveRow> resultsToMerge = new ArrayList<GeoWaveRow>();

		if ((mergingAdapter != null) && (mergingAdapter.getTransform() != null)) {

			resultsToMerge.add(nextResult);

			// Peek ahead to see if it needs to be merged with the next result
			while (scannerIt.hasNext()) {
				peekedValue = scannerIt.next();

				if (DataStoreUtils.rowIdsMatch(
						nextResult,
						peekedValue)) {
					resultsToMerge.add(peekedValue);
					peekedValue = null;
				}
				else {
					nextResult = mergeResults(
							mergingAdapter,
							resultsToMerge);
					return nextResult;
				}
			}
			// If the last results in the scanner are mergeable, merge them
			nextResult = mergeResults(
					mergingAdapter,
					resultsToMerge);
		}

		return nextResult;
	}

	private GeoWaveRow mergeResults(
			final RowMergingDataAdapter mergingAdapter,
			final ArrayList<GeoWaveRow> resultsToMerge ) {
		if (resultsToMerge.isEmpty()) {
			return null;
		}
		else if (resultsToMerge.size() == 1) {
			final GeoWaveRow row = resultsToMerge.get(0);
			return new GeoWaveRowImpl(
					new GeoWaveKeyImpl(
							DataStoreUtils.removeUniqueId(row.getDataId()),
							row.getAdapterId(),
							row.getPartitionKey(),
							row.getSortKey(),
							row.getNumberOfDuplicates()),
					row.getFieldValues());
		}

		Collections.sort(
				resultsToMerge,
				new Comparator<GeoWaveRow>() {
					@Override
					public int compare(
							final GeoWaveRow row1,
							final GeoWaveRow row2 ) {

						final ByteBuffer buf1 = ByteBuffer.wrap(row1.getDataId());
						final ByteBuffer buf2 = ByteBuffer.wrap(row2.getDataId());
						buf1.position((buf1.remaining() - DataStoreUtils.UNIQUE_ADDED_BYTES) + 1);
						buf2.position((buf2.remaining() - DataStoreUtils.UNIQUE_ADDED_BYTES) + 1);

						final long ts1 = buf1.getLong();
						final long ts2 = buf2.getLong();

						return Long.compare(
								ts2,
								ts1);
					}

				});

		final Iterator<GeoWaveRow> iter = resultsToMerge.iterator();
		GeoWaveRow mergedResult = iter.next();
		while (iter.hasNext()) {
			mergedResult = merge(
					mergingAdapter,
					mergedResult,
					iter.next());
		}

		resultsToMerge.clear();
		return mergedResult;
	}

	private GeoWaveRow merge(
			final RowMergingDataAdapter mergingAdapter,
			final GeoWaveRow row,
			final GeoWaveRow rowToMerge ) {

		RowTransform transform = transforms.get(mergingAdapter.getAdapterId());
		if (transform == null) {
			transform = mergingAdapter.getTransform();
			// set strategy
			try {
				transform.initOptions(mergingAdapter.getOptions(null));
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

		// merge values into a single value
		final GeoWaveValue value = new GeoWaveValueImpl(
				row.getFieldValues());
		final GeoWaveValue valueToMerge = new GeoWaveValueImpl(
				rowToMerge.getFieldValues());

		final Mergeable mergeable = transform.getRowAsMergeableObject(
				new ByteArrayId(
						row.getAdapterId()),
				new ByteArrayId(
						value.getFieldMask()),
				value.getValue());

		mergeable.merge(transform.getRowAsMergeableObject(
				new ByteArrayId(
						rowToMerge.getAdapterId()),
				new ByteArrayId(
						valueToMerge.getFieldMask()),
				valueToMerge.getValue()));
		// the assumption is that row and rowToMerge have the same keys, field
		// mask, and visibility
		// this should be a valid assumption
		return new GeoWaveRowImpl(
				new GeoWaveKeyImpl(
						DataStoreUtils.removeUniqueId(row.getDataId()),
						row.getAdapterId(),
						row.getPartitionKey(),
						row.getSortKey(),
						row.getNumberOfDuplicates()),
				new GeoWaveValue[] {
					new GeoWaveValueImpl(
							value.getFieldMask(),
							value.getVisibility(),
							transform.getBinaryFromMergedObject(mergeable))
				});

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
