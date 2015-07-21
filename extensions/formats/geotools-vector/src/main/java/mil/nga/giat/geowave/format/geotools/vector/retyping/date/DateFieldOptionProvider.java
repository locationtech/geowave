package mil.nga.giat.geowave.format.geotools.vector.retyping.date;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

public class DateFieldOptionProvider implements
		IngestFormatOptionProvider
{
	private final static Logger LOGGER = Logger.getLogger(DateFieldOptionProvider.class);

	private Map<String, String> fieldToFormatMap = null;

	@Override
	public void applyOptions(
			Options allOptions ) {
		final Option dateOption = new Option(
				"date",
				true,
				"A map of date field names to the date format of the file. Use commas to separate each entry, then the first ':' character will separate the field name from the format. Use '\\,' to include a comma in the format. For example: \"time:MM:dd:YYYY,time2:YYYY/MM/dd hh:mm:ss\" configures fields 'time' and 'time2' as dates with different formats");
		allOptions.addOption(dateOption);
	}

	@Override
	public void parseOptions(
			CommandLine commandLine ) {
		if (commandLine.hasOption("date")) {
			fieldToFormatMap = new HashMap<>();
			final String arg = commandLine.getOptionValue("date");
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
							LOGGER.error("Field entry: \"" + entry + "\" requires semi-colon to separate field Name from field Format");
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
		}
	}

	public Map<String, String> getFieldToFormatMap() {
		return fieldToFormatMap;
	}
}
