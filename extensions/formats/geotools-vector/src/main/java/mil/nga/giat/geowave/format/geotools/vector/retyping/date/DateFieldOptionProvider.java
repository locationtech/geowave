package mil.nga.giat.geowave.format.geotools.vector.retyping.date;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

public class DateFieldOptionProvider
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DateFieldOptionProvider.class);

	@Parameter(names = "--data", description = "A map of date field names to the date format of the file. Use commas to separate each entry, then the first ':' character will separate the field name from the format. Use '\\,' to include a comma in the format. For example: \"time:MM:dd:YYYY,time2:YYYY/MM/dd hh:mm:ss\" configures fields 'time' and 'time2' as dates with different formats", converter = StringToDateFieldConverter.class)
	private Map<String, String> fieldToFormatMap = null;

	public Map<String, String> getFieldToFormatMap() {
		return fieldToFormatMap;
	}

	/**
	 * Class to convert from a String to Map
	 */
	public static class StringToDateFieldConverter implements
			IStringConverter<Map<String, String>>
	{
		@Override
		public Map<String, String> convert(
				final String arg ) {
			Map<String, String> fieldToFormatMap = new HashMap<>();
			if (arg != null) {
				String[] values = arg.split(",");
				StringBuilder escapedStrs = new StringBuilder();
				for (String entryRaw : values) {
					if (entryRaw.endsWith("\\")) {
						escapedStrs.append(entryRaw.substring(
								0,
								entryRaw.length() - 1) + ",");
					}
					else {
						final String entry = escapedStrs.toString() + entryRaw;
						escapedStrs = new StringBuilder();

						final int firstSemiCol = entry.indexOf(':');
						if (firstSemiCol < 0) {
							LOGGER.error("Field entry: \"" + entry
									+ "\" requires semi-colon to separate field Name from field Format");
						}
						else {
							final String fieldName = entry.substring(
									0,
									firstSemiCol).trim();
							final String fieldValue = entry.substring(
									firstSemiCol + 1).trim();
							LOGGER.debug("TRANSFORMATION: " + fieldName + " --- " + fieldValue);
							fieldToFormatMap.put(
									fieldName,
									fieldValue);
						}
					}
				}
			}
			return fieldToFormatMap;
		}
	}

}
