package mil.nga.giat.geowave.core.index;

import java.util.Map;

public class ClassCompatabilityFactory
{
	// private static final String legacyPackage = "mil.nga.giat.geowave";
	// private static final String futurePackage = "org.locationtech.geowave";

	public static byte[] getClassIdentifierFromClassName(
			final String className )
			throws Exception {
		int classNameIdentifier = 0;
		if (getClassIdentifiersMap().containsKey(
				className)) {
			classNameIdentifier = getClassIdentifiersMap().get(
					className);
			String classNameIdentifierRaw = Integer.toString(classNameIdentifier);
			if (classNameIdentifierRaw != null) {
				classNameIdentifierRaw = classNameIdentifierRaw.trim();
			}
			return StringUtils.stringToBinary(classNameIdentifierRaw);
		}
		else {
			return StringUtils.stringToBinary(className);
		}

	}

	public static String getClassNameFromClassIdentifier(
			final byte[] classNameBinary ) {
		String className = null;
		if (classNameBinary != null && classNameBinary.length != 0) {
			String classIdentifierRaw = StringUtils.stringFromBinary(classNameBinary);
			try {
				// verify value is a numeric
				Integer classNameIdentifier = Integer.valueOf(classIdentifierRaw);
				className = getClassNamesMap().getOrDefault(
						classNameIdentifier,
						null);
			}
			catch (NumberFormatException nfEx) {
				className = classIdentifierRaw;
			}
		}
		return className;
	}

	/**
	 * @return the classNameHashes
	 */
	private static Map<Integer, String> getClassNamesMap() {
		return ClassNameIdentifierRegistry.classNames;
	}

	private static Map<String, Integer> getClassIdentifiersMap() {
		return ClassNameIdentifierRegistry.classNameIdentifiers;
	}
}
