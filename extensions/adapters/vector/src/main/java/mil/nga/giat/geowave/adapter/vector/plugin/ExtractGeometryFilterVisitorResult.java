/**
 * 
 */
package mil.nga.giat.geowave.adapter.vector.plugin;

import com.vividsolutions.jts.geom.Geometry;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;

/**
 * @author Ashish Shah
 *
 *         This class is used to store results extracted from
 *         ExtractGeometryFilterVisitor class. It simply stores query geometry
 *         and its associated predicate.
 */
public final class ExtractGeometryFilterVisitorResult
{
	private final Geometry geometry;
	private final CompareOperation compareOp;

	public ExtractGeometryFilterVisitorResult(
			Geometry geometry,
			CompareOperation compareOp ) {
		this.geometry = geometry;
		this.compareOp = compareOp;
	}

	/**
	 * @return geometry
	 */
	public Geometry getGeometry() {
		return geometry;
	}

	/**
	 * @return predicate associated with geometry
	 */
	public CompareOperation getCompareOp() {
		return compareOp;
	}

	/**
	 * @param otherResult
	 *            is ExtractGeometryFilterVisitorResult object
	 * @return True if predicates of both ExtractGeometryFilterVisitorResult
	 *         objects are same
	 */
	public boolean matchPredicate(
			final ExtractGeometryFilterVisitorResult otherResult ) {
		return (this.compareOp == otherResult.getCompareOp());
	}
}
