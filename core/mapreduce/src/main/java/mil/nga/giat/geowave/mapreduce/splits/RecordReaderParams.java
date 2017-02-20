package mil.nga.giat.geowave.mapreduce.splits;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.BaseReaderParams;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

public class RecordReaderParams extends
		BaseReaderParams
{
	private final GeoWaveRowRange rowRange;

	public RecordReaderParams(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldSubsets,
			final boolean isMixedVisibility,
			final GeoWaveRowRange rowRange,
			final Integer limit,
			final String... additionalAuthorizations ) {
		super(
				index,
				adapterIds,
				maxResolutionSubsamplingPerDimension,
				aggregation,
				fieldSubsets,
				isMixedVisibility,
				limit,
				additionalAuthorizations);
		this.rowRange = rowRange;
	}

	public GeoWaveRowRange getRowRange() {
		return rowRange;
	}
}
