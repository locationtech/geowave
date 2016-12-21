package mil.nga.giat.geowave.datastore.dynamodb.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBIndexWriter;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;

/**
 * This class is used internally to perform query operations against an DynamoDB
 * data store. The query is defined by the set of parameters passed into the
 * constructor.
 */
abstract public class DynamoDBQuery
{
	private final static Logger LOGGER = Logger.getLogger(DynamoDBQuery.class);
	protected final List<ByteArrayId> adapterIds;
	protected final PrimaryIndex index;
	protected final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair;
	protected final DifferingFieldVisibilityEntryCount visibilityCounts;
	final DynamoDBOperations dynamodbOperations;

	private final String[] authorizations;

	public DynamoDBQuery(
			final DynamoDBOperations dynamodbOperations,
			final PrimaryIndex index,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this(
				dynamodbOperations,
				null,
				index,
				null,
				visibilityCounts,
				authorizations);
	}

	public DynamoDBQuery(
			final DynamoDBOperations dynamodbOperations,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this.dynamodbOperations = dynamodbOperations;
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

	protected Iterator<Map<String, AttributeValue>> getResults(
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		final List<ByteArrayRange> ranges = getRanges();
		final String tableName = dynamodbOperations.getQualifiedTableName(
				StringUtils.stringFromBinary(
						index.getId().getBytes()));
		final List<QueryRequest> requests = new ArrayList<>();
		if (ranges != null) {
			if (ranges.size() == 1) {
				final List<QueryRequest> queries = getPartitionRequests(
						tableName);
				final ByteArrayRange r = ranges.get(
						0);
				if (r.isSingleValue()) {
					for (final QueryRequest query : queries) {
						query.addQueryFilterEntry(
								DynamoDBRow.GW_IDX_KEY,
								new Condition()
										.withAttributeValueList(
												new AttributeValue().withB(
														ByteBuffer.wrap(
																r.getStart().getBytes())))
										.withComparisonOperator(
												ComparisonOperator.EQ));
					}
				}
				else {
					for (final QueryRequest query : queries) {
						addQueryRange(
								r,
								query);
					}
				}
				requests.addAll(
						queries);
			}
			ranges.forEach(
					(r -> requests.addAll(
							addQueryRanges(
									tableName,
									r))));
		}

		return requests
				.parallelStream()
				.map(
						this::executeQueryRequest)
				.flatMap(
						List::stream)
				.iterator();

	}

	private List<QueryRequest> addQueryRanges(
			final String tableName,
			final ByteArrayRange r ) {
		final List<QueryRequest> retVal = getPartitionRequests(tableName);
		for (final QueryRequest queryRequest : retVal) {
			addQueryRange(
					r,
					queryRequest);
		}
		return retVal;
	}

	private void addQueryRange(
			final ByteArrayRange r,
			final QueryRequest query ) {
		query.addKeyConditionsEntry(
				DynamoDBRow.GW_IDX_KEY,
				new Condition().withComparisonOperator(
						ComparisonOperator.BETWEEN).withAttributeValueList(
						new AttributeValue().withB(ByteBuffer.wrap(r.getStart().getBytes())),
						new AttributeValue().withB(ByteBuffer.wrap(r.getEndAsNextPrefix().getBytes()))));
	}

	private static List<QueryRequest> getPartitionRequests(
			final String tableName ) {
		final List<QueryRequest> requests = new ArrayList<>(
				DynamoDBIndexWriter.PARTITIONS);
		for (int p = 0; p < DynamoDBIndexWriter.PARTITIONS; p++) {
			requests.add(new QueryRequest(
					tableName).addKeyConditionsEntry(
					DynamoDBRow.GW_PARTITION_ID_KEY,
					new Condition().withComparisonOperator(
							ComparisonOperator.EQ).withAttributeValueList(
							new AttributeValue().withN(Integer.toString(p)))));
		}
		return requests;
	}

	private List<Map<String, AttributeValue>> executeQueryRequest(
			final QueryRequest queryRequest ) {
		return dynamodbOperations.getClient().query(
				queryRequest).getItems();
	}

	public String[] getAdditionalAuthorizations() {
		return authorizations;
	}
}
