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
			Class<?> factoryType = null;

			try {
				factoryType = Class.forName(className);
			}
			catch (final ClassNotFoundException e) {
				LOGGER.warn(
						"error creating class: could not find class ",
						e);
			}

			if (factoryType != null) {
				try {
					// use the no arg constructor and make sure its accessible

					// HP Fortify "Access Specifier Manipulation"
					// This method is being modified by trusted code,
					// in a way that is not influenced by user input
					final Constructor<?> noArgConstructor = factoryType.getDeclaredConstructor();
					noArgConstructor.setAccessible(true);
					factoryClassInst = noArgConstructor.newInstance();

					getFactoryMap().put(
							className,
							factoryClassInst);
				}
				catch (final Exception e) {
					LOGGER.warn(
							"error creating class: could not create class ",
							e);
				}
			}
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
