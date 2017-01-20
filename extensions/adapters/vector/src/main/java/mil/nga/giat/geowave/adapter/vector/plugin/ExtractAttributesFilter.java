package mil.nga.giat.geowave.adapter.vector.plugin;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.geotools.filter.visitor.DefaultFilterVisitor;
import org.opengis.filter.expression.PropertyName;

/**
 * This class can be used to get the list of attributes used in a query
 *
 */
public class ExtractAttributesFilter extends
		DefaultFilterVisitor
{

	public ExtractAttributesFilter() {}

	@Override
	public Object visit(
			final PropertyName expression,
			final Object data ) {
		if ((data != null) && (data instanceof Collection)) {
			((Collection) data).add(expression.getPropertyName());
			return data;
		}
		final Set<String> names = new HashSet<String>();
		names.add(expression.getPropertyName());
		return names;
	}

}
