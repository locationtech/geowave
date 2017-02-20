package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.SortedIndexStrategy;

/**
 * Interface which defines an index strategy.
 *
 */
public interface FieldIndexStrategy<ConstraintType extends FilterableConstraints, FieldType> extends
		SortedIndexStrategy<ConstraintType, FieldType>
{

}
