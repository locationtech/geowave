package mil.nga.giat.geowave.datastore.accumulo.query;

import java.util.List;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

/**
 * This class is used internally to perform query operations against an Accumulo
 * data store. The query is defined by the set of parameters passed into the
 * constructor.
 */
abstract public class AccumuloQuery
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloQuery.class);
	protected final List<ByteArrayId> adapterIds;
	protected final PrimaryIndex index;
	protected final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair;
	protected final DifferingFieldVisibilityEntryCount visibilityCounts;

	private final String[] authorizations;

	public AccumuloQuery(
			final PrimaryIndex index,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this(
				null,
				index,
				null,
				visibilityCounts,
				authorizations);
	}

	public AccumuloQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this.adapterIds = adapterIds;
		this.index = index;
		this.fieldIdsAdapterPair = fieldIdsAdapterPair;
		this.visibilityCounts = visibilityCounts;
		this.authorizations = authorizations;
	}

	abstract protected List<ByteArrayRange> getRanges();

	protected boolean isAggregation() {
		return false;
	}

	protected boolean useWholeRowIterator() {
		return (visibilityCounts == null) || visibilityCounts.isAnyEntryDifferingFieldVisiblity();
	}

	protected ScannerBase createScanner(
			final AccumuloOperations accumuloOperations,
			String tableName,
			boolean batchScanner,
			String... authorizations )
			throws TableNotFoundException {
		if (batchScanner) {
			return accumuloOperations.createBatchScanner(
					tableName,
					authorizations);
		}
		return accumuloOperations.createScanner(
				tableName,
				authorizations);
	}

	protected ScannerBase getScanner(
			final AccumuloOperations accumuloOperations,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		final List<ByteArrayRange> ranges = getRanges();
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		ScannerBase scanner;
		try {
			scanner = createScanner(
					accumuloOperations,
					tableName,
					isAggregation() || (ranges == null) || (ranges.size() != 1),
					getAdditionalAuthorizations());
			if (scanner instanceof Scanner) {
				final ByteArrayRange r = ranges.get(0);
				if (r.isSingleValue()) {
					((Scanner) scanner).setRange(Range.exact(new Text(
							r.getStart().getBytes())));
				}
				else {
					((Scanner) scanner).setRange(AccumuloUtils.byteArrayRangeToAccumuloRange(r));
				}
				if ((limit != null) && (limit > 0) && (limit < ((Scanner) scanner).getBatchSize())) {
					// do allow the limit to be set to some enormous size.
					((Scanner) scanner).setBatchSize(Math.min(
							1024,
							limit));
				}
			}
			else if (scanner instanceof BatchScanner) {
				((BatchScanner) scanner).setRanges(AccumuloUtils.byteArrayRangesToAccumuloRanges(ranges));
			}
			if (maxResolutionSubsamplingPerDimension != null) {
				if (maxResolutionSubsamplingPerDimension.length != index
						.getIndexStrategy()
						.getOrderedDimensionDefinitions().length) {
					LOGGER.warn("Unable to subsample for table '" + tableName + "'. Subsample dimensions = "
							+ maxResolutionSubsamplingPerDimension.length + " when indexed dimensions = "
							+ index.getIndexStrategy().getOrderedDimensionDefinitions().length);
				}
				else {

					final int cardinalityToSubsample = (int) Math.round(IndexUtils.getDimensionalBitsUsed(
							index.getIndexStrategy(),
							maxResolutionSubsamplingPerDimension)
							+ (8 * index.getIndexStrategy().getByteOffsetFromDimensionalIndex()));

					final IteratorSetting iteratorSettings = new IteratorSetting(
							FixedCardinalitySkippingIterator.CARDINALITY_SKIPPING_ITERATOR_PRIORITY,
							FixedCardinalitySkippingIterator.CARDINALITY_SKIPPING_ITERATOR_NAME,
							FixedCardinalitySkippingIterator.class);
					iteratorSettings.addOption(
							FixedCardinalitySkippingIterator.CARDINALITY_SKIP_INTERVAL,
							Integer.toString(cardinalityToSubsample));
					scanner.addScanIterator(iteratorSettings);
				}
			}
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
			return null;
		}
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			for (final ByteArrayId adapterId : adapterIds) {
				scanner.fetchColumnFamily(new Text(
						adapterId.getBytes()));
			}
		}
		return scanner;
	}

	protected void addFieldSubsettingToIterator(
			final ScannerBase scanner ) {
		if ((fieldIdsAdapterPair != null) && !isAggregation()) {
			final List<String> fieldIds = fieldIdsAdapterPair.getLeft();
			final DataAdapter<?> associatedAdapter = fieldIdsAdapterPair.getRight();
			if ((fieldIds != null) && (!fieldIds.isEmpty()) && (associatedAdapter != null)) {
				final IteratorSetting iteratorSetting = AttributeSubsettingIterator.getIteratorSetting();

				AttributeSubsettingIterator.setFieldIds(
						iteratorSetting,
						associatedAdapter,
						fieldIds,
						index.getIndexModel());

				iteratorSetting.addOption(
						AttributeSubsettingIterator.WHOLE_ROW_ENCODED_KEY,
						Boolean.toString(useWholeRowIterator()));
				scanner.addScanIterator(iteratorSetting);
			}
		}
	}

	public String[] getAdditionalAuthorizations() {
		return authorizations;
	}
}
