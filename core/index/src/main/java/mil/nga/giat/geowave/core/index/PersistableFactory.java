package mil.nga.giat.geowave.core.index;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistableFactory
{
	private final static Logger LOGGER = LoggerFactory.getLogger(PersistableFactory.class);

	private static Map<String, Object> factoryMap;

	@SuppressWarnings("unchecked")
	public static <T> T getPersistable(
			final String className,
			Class<T> expectedType ) {
		Object factoryClassInst = getFactoryMap().getOrDefault(
				className,
				null);
		if (factoryClassInst == null) {
			try {
				Class<?> factoryType = Class.forName(className);
				if (factoryType != null) {
					Constructor<?> noArgConstructor = factoryType.getDeclaredConstructor();
					noArgConstructor.setAccessible(true);
					factoryClassInst = noArgConstructor.newInstance();
				}
			}
			catch (final Exception e) {
				LOGGER.warn(
						"error creating class: could not find class ",
						e);
			}
			getFactoryMap().put(
					className,
					factoryClassInst);
		}
		if (factoryClassInst != null) {
			if (!expectedType.isAssignableFrom(factoryClassInst.getClass())) {
				LOGGER.warn("error creating class, does not implement expected type");
			}
			else {
				if (factoryClassInst instanceof Persistable) {
					Persistable persistable = ((Persistable) factoryClassInst).getPersistable();
					if (persistable != null) {
						return (T) persistable;
					}
				}
				return (T) factoryClassInst;
			}
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
