package mil.nga.giat.geowave.vector.plugin.visibility;


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

}
