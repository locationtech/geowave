package mil.nga.giat.geowave.adapter.vector.query.cql;

import org.geotools.filter.visitor.NullFilterVisitor;
import org.opengis.filter.spatial.DWithin;

public class HasDWithinFilterVisitor extends
		NullFilterVisitor
{
	private boolean hasDWithin = false;

	@Override
	public Object visit(
			DWithin filter,
			Object data ) {
		hasDWithin = true;
		return super.visit(
				filter,
				data);
	}

	public boolean hasDWithin() {
		return hasDWithin;
	}
}
