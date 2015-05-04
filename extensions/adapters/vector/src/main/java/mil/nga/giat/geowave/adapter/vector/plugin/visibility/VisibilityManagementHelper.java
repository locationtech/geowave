package mil.nga.giat.geowave.adapter.vector.plugin.visibility;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * At the moment, the expectation is that a single GeoServer instance supports
 * only one visibility management approach/format.
 * 
 * 
 * 
 * 
 */
public class VisibilityManagementHelper
{

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static final <T> ColumnVisibilityManagement<T> loadVisibilityManagement() {
		ServiceLoader<ColumnVisibilityManagement> ldr = ServiceLoader.load(ColumnVisibilityManagement.class);
		Iterator<ColumnVisibilityManagement> managers = ldr.iterator();
		if (!managers.hasNext()) return new JsonDefinitionColumnVisibilityManagement<T>();
		return (ColumnVisibilityManagement<T>) managers.next();
	}
}
