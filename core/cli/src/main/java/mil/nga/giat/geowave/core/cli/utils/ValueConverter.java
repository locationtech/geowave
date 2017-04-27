/**
 * 
 */
package mil.nga.giat.geowave.core.cli.utils;

import org.apache.commons.beanutils.ConvertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * Used for general purpose value conversion via appache commons ConvertUtils
 */
public class ValueConverter
{
	private static Logger LOGGER = LoggerFactory.getLogger(ValueConverter.class);

	/**
	 * Private constructor to prevent accidental instantiation
	 */
	private ValueConverter() {}

	/**
	 * Convert value into the specified type
	 * 
	 * @param <X>
	 *            Class to convert to
	 * @param value
	 *            Value to convert from
	 * @param targetType
	 *            Type to convert into
	 * @return The converted value
	 */
	@SuppressWarnings("unchecked")
	public static <X> X convert(
			Object value,
			Class<X> targetType ) {
		LOGGER.trace("Attempting to convert " + value + " to class type " + targetType);
		if (value != null) {
			// if object is already in intended target type, no need to convert
			// it, just return as it is
			if (value.getClass() == targetType) {
				return (X) value;
			}

			if (value.getClass() == JSONObject.class || value.getClass() == JSONArray.class) {
				return (X) value;
			}
		}

		String strValue = String.valueOf(value);
		Object retval = ConvertUtils.convert(
				strValue,
				targetType);
		return (X) retval;
	}
}