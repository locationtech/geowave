package mil.nga.giat.geowave.store.query;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;

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
			final TemporalConstraints contraints ) {
		final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerDimension = new HashMap<Class<? extends NumericDimensionDefinition>, ConstraintData>();
		// Create and return a new IndexRange array with an x and y axis
		// range
		for (final TemporalRange range : contraints.getRanges()) {
			constraintsPerDimension.put(
					TimeDefinition.class,
					new ConstraintData(
							new NumericRange(
									range.getStartTime().getTime(),
									range.getEndTime().getTime()),
							false));
		}

		final Constraints constraints = new Constraints(
				constraintsPerDimension);

		return constraints;
	}

}
