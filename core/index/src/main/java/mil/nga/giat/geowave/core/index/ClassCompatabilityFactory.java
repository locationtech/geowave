package mil.nga.giat.geowave.core.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassCompatabilityFactory
{
	private final static Logger LOGGER = LoggerFactory.getLogger(ClassCompatabilityFactory.class);

	private static final String legacyPackage = "mil.nga.giat.geowave";
	private static final String futurePackage = "org.locationtech.geowave";

	private static Map<String, byte[]> unregisteredClassNames;

	/**
	 * Given a class name and the desired/expected type, return a compatible
	 * class name between the two class names
	 *
	 * @param className
	 * @param expectedTypeClassName
	 * @return
	 */
	public static String lookupCompatibleClassName(
			final String className,
			final String expectedTypeClassName ) {
		if (className.equals(expectedTypeClassName)) {
			return className;
		}

		Class<?> originalClass = null, expectedClass = null;
		try {
			originalClass = Class.forName(className);
			expectedClass = Class.forName(expectedTypeClassName);
		}
		catch (Exception ex) {
			LOGGER.error(
					"ClassNotFoundException Error: " + ex.getLocalizedMessage(),
					ex);
		}
		if (originalClass != null && expectedClass != null && expectedClass.isAssignableFrom(originalClass)) {
			return className;
		}

		String compatibleClassName = className;
		if (compatibleClassName.startsWith(legacyPackage) && expectedTypeClassName.startsWith(futurePackage)) {
			// if migrating from legacy to future
			compatibleClassName = compatibleClassName.replace(
					legacyPackage,
					futurePackage);
		}
		else if (compatibleClassName.startsWith(futurePackage) && expectedTypeClassName.startsWith(legacyPackage)) {
			// if converting from future back to legacy - if there was a
			// use-case for this scenario
			compatibleClassName = compatibleClassName.replace(
					futurePackage,
					legacyPackage);
		}
		return compatibleClassName;
	}

	/**
	 * Given a class name, return the binary representation of a class
	 * identifier, if the class was registered. Otherwise, return the binary
	 * representation of the class name
	 * 
	 * @param className
	 * @return
	 * @throws Exception
	 */
	public static byte[] getClassIdentifierFromClassName(
			final String className )
			throws Exception {
		if (className == null || "".equals(className.trim())) {
			return new byte[0];
		}

		short classNameIdentifier = 0;
		if (getClassIdentifiersMap().containsKey(
				className)) {
			classNameIdentifier = getClassIdentifiersMap().get(
					className);
			String classNameIdentifierRaw = Short.toString(classNameIdentifier);
			if (classNameIdentifierRaw != null) {
				classNameIdentifierRaw = classNameIdentifierRaw.trim();
			}
			return StringUtils.stringToBinary(classNameIdentifierRaw);
		}
		else {
			// check if unregistered class name was cached
			byte[] unregisteredClassNameBinary = getUnregisteredClassNames().getOrDefault(
					className,
					null);
			if (unregisteredClassNameBinary == null) {
				// if a class was not registered, rather than continually
				// re-generating the binary, we'll store within a map for faster
				LOGGER
						.warn(
								"Class [{}] was not registered within {} registry. For more efficient performance, please register.",
								new Object[] {
									className,
									ClassNameIdentifierRegistry.class.getName()
								});
				unregisteredClassNameBinary = StringUtils.stringToBinary(className);
				registerClassIdentifier(
						className,
						unregisteredClassNameBinary);
			}
			return unregisteredClassNameBinary;
		}
	}

	private static void registerClassIdentifier(
			String className,
			byte[] unregisteredClassNameBinary ) {
		if (className != null && !"".equals(className.trim()) && !getUnregisteredClassNames().containsKey(
				className)) {
			getUnregisteredClassNames().put(
					className,
					unregisteredClassNameBinary);
		}
	}

	/**
	 * Given a class name binary value, convert it to a string and return the
	 * class name, either from an identifier lookup (if registered and numeric)
	 * or the specific class name itself (if not registered)
	 * 
	 * @param classNameBinary
	 * @return
	 */
	public static String getClassNameFromClassIdentifier(
			final byte[] classNameBinary ) {
		if (classNameBinary == null || classNameBinary.length == 0) {
			return null;
		}

		String classIdentifierRaw = StringUtils.stringFromBinary(classNameBinary);
		try {
			// verify value is a short
			Short classNameIdentifier = Short.valueOf(classIdentifierRaw);
			return getClassNamesMap().getOrDefault(
					classNameIdentifier,
					null);
		}
		catch (NumberFormatException nfEx) {
			// exception will be thrown if not a number
			return classIdentifierRaw;
		}
	}

	/**
	 * @return the classNameHashes
	 */
	private static Map<Short, String> getClassNamesMap() {
		return ClassNameIdentifierRegistry.getClassNames();
	}

	private static Map<String, Short> getClassIdentifiersMap() {
		return ClassNameIdentifierRegistry.getClassNameIdentifiers();
	}

	public static Map<String, byte[]> getUnregisteredClassNames() {
		if (unregisteredClassNames == null) {
			unregisteredClassNames = Collections.synchronizedMap(new HashMap<String, byte[]>());
		}
		return unregisteredClassNames;
	}
}
