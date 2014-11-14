package mil.nga.giat.geowave.types;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class HelperClass
{

	public static <T> Set<String> buildSet(
			Map<String, ValidateObject<T>> expectedResults ) {
		HashSet<String> set = new HashSet<String>();
		for (String key : expectedResults.keySet()) {
			set.add(key);
		}
		return set;
	}
}
