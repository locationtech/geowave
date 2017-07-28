package mil.nga.giat.geowave.core.store.util;

import java.io.IOException;
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

	public MergingEntryIterator(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<GeoWaveRow> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T, GeoWaveRow> scanCallback,
			final Map<ByteArrayId, RowMergingDataAdapter> mergingAdapters,
			final double[] maxResolutionSubsamplingPerDimension ) {
		super(
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback,
				null,
				maxResolutionSubsamplingPerDimension,
				true);
		this.mergingAdapters = mergingAdapters;
		transforms = new HashMap<ByteArrayId, RowTransform>();
	}

	protected GeoWaveRow getNextEncodedResult() {
		GeoWaveRow nextResult = scannerIt.next();

		final ByteArrayId adapterId = new ByteArrayId(
				nextResult.getAdapterId());

		final RowMergingDataAdapter mergingAdapter = mergingAdapters.get(adapterId);

		if ((mergingAdapter != null) && (mergingAdapter.getTransform() != null)) {
			final RowTransform rowTransform = getRowTransform(mergingAdapter);

			// This iterator expects a single GeoWaveRow w/ multiple fieldValues
			// (HBase)
			nextResult = mergeSingleRowValues(
					nextResult,
					rowTransform);
		}

		return nextResult;
	}

	private RowTransform getRowTransform(
			RowMergingDataAdapter mergingAdapter ) {
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

		return transform;
	}

	private GeoWaveRow mergeSingleRowValues(
			final GeoWaveRow singleRow,
			final RowTransform rowTransform ) {
		if (singleRow.getFieldValues().length < 2) {
			return singleRow;
		}

		// merge all values into a single value
		Mergeable merged = null;

		for (GeoWaveValue fieldValue : singleRow.getFieldValues()) {
			final Mergeable mergeable = rowTransform.getRowAsMergeableObject(
					new ByteArrayId(
							singleRow.getAdapterId()),
					new ByteArrayId(
							fieldValue.getFieldMask()),
					fieldValue.getValue());

			if (merged == null) {
				merged = mergeable;
			}
			else {
				merged.merge(mergeable);
			}
		}

		GeoWaveValue[] mergedFieldValues = new GeoWaveValue[] {
			new GeoWaveValueImpl(
					singleRow.getFieldValues()[0].getFieldMask(),
					singleRow.getFieldValues()[0].getVisibility(),
					rowTransform.getBinaryFromMergedObject(merged))
		};

		return new GeoWaveRowImpl(
				new GeoWaveKeyImpl(
						singleRow.getDataId(),
						singleRow.getAdapterId(),
						singleRow.getPartitionKey(),
						singleRow.getSortKey(),
						singleRow.getNumberOfDuplicates()),
				mergedFieldValues);
	}

	@Override
	protected boolean hasNextScannedResult() {
		return scannerIt.hasNext();
	}

	@Override
	public void remove() {
		throw new NotImplementedException(
				"Transforming iterator cannot use remove()");
	}

}
