package mil.nga.giat.geowave.adapter.vector.plugin.visibility;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	protected final static Logger LOGGER = LoggerFactory.getLogger(VisibilityManagementHelper.class);

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static final <T> ColumnVisibilityManagementSpi<T> loadVisibilityManagement() {
		ServiceLoader<ColumnVisibilityManagementSpi> ldr = ServiceLoader.load(ColumnVisibilityManagementSpi.class);
		Iterator<ColumnVisibilityManagementSpi> managers = ldr.iterator();
		if (!managers.hasNext()) return new JsonDefinitionColumnVisibilityManagement<T>();
		return (ColumnVisibilityManagementSpi<T>) managers.next();
	}
}
