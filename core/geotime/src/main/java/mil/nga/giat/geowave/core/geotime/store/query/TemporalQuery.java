package mil.nga.giat.geowave.core.geotime.store.query;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.query.BasicQuery;

/**
 * The Spatial Temporal Query class represents a query in three dimensions. The
 * constraint that is applied represents an intersection operation on the query
 * geometry AND a date range intersection based on startTime and endTime.
 * 
 * 
 */
public class TemporalQuery extends
		BasicQuery
{

	public TemporalQuery(
			final TemporalConstraints contraints ) {
		super(
				createTemporalConstraints(contraints));
	}

	protected TemporalQuery() {
		super();
	}

	private static Constraints createTemporalConstraints(
			final TemporalConstraints temporalConstraints ) {
		final List<ConstraintSet> constraints = new ArrayList<ConstraintSet>();
		for (final TemporalRange range : temporalConstraints.getRanges()) {
			constraints.add(new ConstraintSet(
					TimeDefinition.class,
					new ConstraintData(
							new NumericRange(
									range.getStartTime().getTime(),
									range.getEndTime().getTime()),
							false)));
		}
		return new Constraints(
				constraints);
	}

}
