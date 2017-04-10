package mil.nga.giat.geowave.datastore.dynamodb.query;

import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.NativeEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;

/**
 * Represents a query operation by an DynamoDB row. This abstraction is
 * re-usable for both exact row ID queries and row prefix queries.
 *
 */
abstract public class AbstractDynamoDBRowQuery<T> extends
		DynamoDBQuery
{
	private static final Logger LOGGER = Logger.getLogger(AbstractDynamoDBRowQuery.class);
	protected final ScanCallback<T, DynamoDBRow> scanCallback;

	public AbstractDynamoDBRowQuery(
			final BaseDataStore dataStore,
			final DynamoDBOperations dynamodbOperations,
			final PrimaryIndex index,
			final String[] authorizations,
			final ScanCallback<T, DynamoDBRow> scanCallback,
			final DifferingFieldVisibilityEntryCount visibilityCounts ) {
		super(
				dataStore,
				dynamodbOperations,
				index,
				visibilityCounts,
				authorizations);

		this.scanCallback = scanCallback;
	}

	public CloseableIterator<T> query(
			final double[] maxResolutionSubsamplingPerDimension,
			final AdapterStore adapterStore ) {
		final Iterator<Map<String, AttributeValue>> results = getResults(
				maxResolutionSubsamplingPerDimension,
				getScannerLimit(),
				adapterStore);
		return new CloseableIterator.Wrapper<T>(
				new NativeEntryIteratorWrapper<>(
						dataStore,
						adapterStore,
						index,
						Iterators.transform(
								results,
								new WrapAsNativeRow()),
						null,
						this.scanCallback,
						true));
	}

	abstract protected Integer getScannerLimit();

	public static class WrapAsNativeRow implements
			Function<Map<String, AttributeValue>, DynamoDBRow>
	{
		@Override
		public DynamoDBRow apply(
				final Map<String, AttributeValue> input ) {
			return new DynamoDBRow(
					input);
		}

	}
}
