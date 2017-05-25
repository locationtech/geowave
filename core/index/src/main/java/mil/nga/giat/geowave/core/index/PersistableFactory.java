package mil.nga.giat.geowave.core.index;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistableFactory
{
	private final static Logger LOGGER = LoggerFactory.getLogger(PersistableFactory.class);

	private static Map<String, Constructor<?>> constructorMap;
	private static Map<String, Class<?>> classMap;

	@SuppressWarnings("unchecked")
	public static <T> T getPersistable(
			final String className,
			Class<T> expectedType ) {
		Constructor<?> noArgConstructor = getConstructorMap().get(
				className);
		if (noArgConstructor == null) {
			Class<?> factoryType = getClassMap().get(
					className);
			if (factoryType == null) {
				try {
					factoryType = Class.forName(className);
				}
				catch (final ClassNotFoundException e) {
					LOGGER.warn(
							"error creating class: could not find class ",
							e);
				}
				getClassMap().put(
						className,
						factoryType);
			}
			if (factoryType != null) {
				try {
					// use the no arg constructor and make sure its accessible

					// HP Fortify "Access Specifier Manipulation"
					// This method is being modified by trusted code,
					// in a way that is not influenced by user input
					noArgConstructor = factoryType.getDeclaredConstructor();
					noArgConstructor.setAccessible(true);
					getConstructorMap().put(
							className,
							noArgConstructor);
				}
				catch (final Exception e) {
					LOGGER.warn(
							"error creating class: could not create class ",
							e);
				}
			}
		}
		if (noArgConstructor != null) {
			Object factoryClassInst = null;
			try {
				factoryClassInst = noArgConstructor.newInstance();
			}
			catch (Exception ex) {
				LOGGER.error(
						ex.getLocalizedMessage(),
						ex);
			}
			if (factoryClassInst != null) {
				if (!expectedType.isAssignableFrom(factoryClassInst.getClass())) {
					LOGGER.warn("error creating class, does not implement expected type");
				}
				else {
					return (T) factoryClassInst;
				}
			}
		}
		return null;
	}

	/**
	 * @return the methodsMap
	 */
	public static Map<String, Constructor<?>> getConstructorMap() {
		if (constructorMap == null) {
			constructorMap = new HashMap<String, Constructor<?>>();
		}
		return constructorMap;
	}

	private static Map<String, Class<?>> getClassMap() {
		if (classMap == null) {
			classMap = new HashMap<String, Class<?>>();
		}
		return classMap;
	}
}
