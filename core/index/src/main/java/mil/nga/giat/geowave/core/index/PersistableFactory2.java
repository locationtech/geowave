package mil.nga.giat.geowave.core.index;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistableFactory2
{
	private final static Logger LOGGER = LoggerFactory.getLogger(PersistableFactory2.class);

	private static Map<String, Object> factoryMap;

	@SuppressWarnings("unchecked")
	public static <T> T getPersistable(
			final String className,
			Class<T> expectedType ) {
		Object factoryObj = getFactoryMap().getOrDefault(
				className,
				null);
		if (factoryObj == null) {
			try {
				Class<?> factoryType = Class.forName(className);
				if (factoryType != null) {
					factoryObj = factoryType.newInstance();
				}
			}
			catch (final Exception e) {
				LOGGER.warn(
						"error creating class: could not find class ",
						e);
			}
			getFactoryMap().put(
					className,
					factoryObj);
		}
		if (factoryObj != null) {
			if (factoryObj instanceof Persistable) {
				Persistable persistable = (Persistable) factoryObj;
				return (T) persistable.getPersistable();
			}
			return (T) factoryObj;
		}
		return null;
	}

	/**
	 * @return the factoryMap
	 */
	public static Map<String, Object> getFactoryMap() {
		if (factoryMap == null) {
			factoryMap = new HashMap<String, Object>();
		}
		return factoryMap;
	}
}
