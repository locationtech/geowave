package mil.nga.giat.geowave.format.geotools.vector.retyping.date;

import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.DynamicParameter;

public class DateFieldOptionProvider
{
	@DynamicParameter(names = "-dateField.", description = "A map of date field names to the date format. Multiple can exist of the form -dateField.<date_field_name>=<date format>. For example this uses different formats for fields named 'time1' and 'time2': dateField.time1=MM:dd:YYYY dateField.time2=\"YYYY/MM/dd hh:mm:ss\"")
	private final Map<String, String> fieldToFormatMap = new HashMap<String, String>();

	public Map<String, String> getFieldToFormatMap() {
		return fieldToFormatMap;
	}
}
