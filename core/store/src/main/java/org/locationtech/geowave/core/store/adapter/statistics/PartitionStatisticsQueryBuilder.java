package org.locationtech.geowave.core.store.adapter.statistics;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionStatisticsQueryBuilder<R> extends
		StatisticsQueryBuilderImpl<R, PartitionStatisticsQueryBuilder<R>>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(PartitionStatisticsQueryBuilder.class);
	private static final String STATS_ID_SEPARATOR = "#";
	private String indexName;
	private ByteArrayId partitionKey;

	public PartitionStatisticsQueryBuilder(
			final StatisticsType<R, PartitionStatisticsQueryBuilder<R>> statsType ) {
		this.statsType = statsType;
	}

	public PartitionStatisticsQueryBuilder<R> indexName(
			final String indexName ) {
		this.indexName = indexName;
		return this;
	}

	public PartitionStatisticsQueryBuilder<R> partition(
			final ByteArrayId partitionKey ) {
		this.partitionKey = partitionKey;
		return this;
	}

	@Override
	protected String extendedId() {
		return composeId(
				indexName,
				partitionKey);
	}

	protected static Pair<String, ByteArrayId> decomposeIndexAndPartitionFromId(
			final String idString ) {
		final int pos = idString.lastIndexOf(STATS_ID_SEPARATOR);
		if (pos < 0) {
			return Pair.of(
					idString,
					null);
		}
		return Pair.of(
				idString.substring(
						0,
						pos),
				new ByteArrayId(
						ByteArrayUtils.byteArrayFromString(idString.substring(pos + 1))));
	}

	protected static String composeId(
			final String indexName,
			final ByteArrayId partitionKey ) {
		if (indexName == null) {
			if ((partitionKey != null) && (partitionKey.getBytes() != null) && (partitionKey.getBytes().length > 0)) {
				LOGGER.warn("Cannot set partitionKey without index. Ignoring Partition Key in statistics query.");
			}
			return null;
		}
		if ((partitionKey == null) || (partitionKey.getBytes() == null) || (partitionKey.getBytes().length == 0)) {
			return indexName;
		}
		return indexName + STATS_ID_SEPARATOR + ByteArrayUtils.byteArrayToString(partitionKey.getBytes());
	}

}
