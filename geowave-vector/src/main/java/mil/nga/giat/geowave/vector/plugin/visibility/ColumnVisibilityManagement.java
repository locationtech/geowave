package mil.nga.giat.geowave.vector.plugin.visibility;

import mil.nga.giat.geowave.accumulo.VisibilityTransformationIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;

/**
 * Treats the visibility column as a String expression, as defined by Accumulo.
 * Access is determined by the visibility column expression such as (a|b)&c.
 * 
 *
 * 
 */
public abstract class ColumnVisibilityManagement<T> implements
		mil.nga.giat.geowave.store.data.visibility.VisibilityManagement<T>
{

	public Class<? extends TransformingIterator> visibilityReplacementIteratorClass() {
		return VisibilityTransformationIterator.class;
	}

}
