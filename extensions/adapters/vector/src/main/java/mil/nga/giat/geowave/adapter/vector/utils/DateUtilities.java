package mil.nga.giat.geowave.adapter.vector.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtilities
{

	public static Date parseISO(
			String input )
			throws java.text.ParseException {

		// NOTE: SimpleDateFormat uses GMT[-+]hh:mm for the TZ which breaks
		// things a bit. Before we go on we have to repair this.
		SimpleDateFormat df = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ssz");

		// this is zero time so we need to add that TZ indicator for
		if (input.endsWith("Z")) {
			input = input.substring(
					0,
					input.length() - 1) + "GMT-00:00";
		}
		else {
			int inset = 6;

			String s0 = input.substring(
					0,
					input.length() - inset);
			String s1 = input.substring(
					input.length() - inset,
					input.length());

			input = s0 + "GMT" + s1;
		}

		return df.parse(input);

	}
}
