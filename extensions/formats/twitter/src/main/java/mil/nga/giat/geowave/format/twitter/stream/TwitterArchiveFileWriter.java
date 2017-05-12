package mil.nga.giat.geowave.format.twitter.stream;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import twitter4j.JSONObject;
import twitter4j.Status;

public class TwitterArchiveFileWriter implements
		TwitterArchiveWriter
{
	private final static Logger LOGGER = Logger.getLogger(
			TwitterArchiveFileWriter.class);

	private Calendar cal;
	private NumberFormat nf;
	private String archivePath;
	private int fileSplits;

	public TwitterArchiveFileWriter(
			final String archivePath,
			final int fileSplits) {
		cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		if (archivePath == null || archivePath.contains(
				"temp")
				|| archivePath.contains(
						"tmp")) {
			this.archivePath = FileUtils.getTempDirectoryPath();
		}
		else {
			this.archivePath = archivePath;
		}

		nf = NumberFormat.getIntegerInstance();
		nf.setMinimumIntegerDigits(
				2);
		nf.setGroupingUsed(
				false);
		
		
		// Clamp file splits to range (1-4)
		this.fileSplits = Math.min(4, Math.max(1, fileSplits));
	}

	@Override
	public void writeTweet(
			final Status status,
			final JSONObject json )
			throws IOException {

		File tweetFile = new File(
				archivePath,
				getArchiveFileNameForDate(
						status.getCreatedAt()));

		String tweetLine = json.toString() + "\n";
		LOGGER.info(
				tweetLine);

		FileUtils.writeStringToFile(
				tweetFile,
				tweetLine,
				true);
	}

	private String getArchiveFileNameForDate(
			Date date ) {
		cal.setTime(
				date);
		int year = cal.get(
				Calendar.YEAR);
		int month = cal.get(
				Calendar.MONTH) + 1;
		int day = cal.get(
				Calendar.DAY_OF_MONTH);
		
		if (fileSplits == 1) {
			return ("tweets-" + nf.format(
					year)
					+ nf.format(
							month)
					+ nf.format(
							day)
					+ "p0"
					+ ".json");
		}
		
		// Split the day; e.g. 00/08/16 if 8-hr split
		int hh = cal.get(
				Calendar.HOUR_OF_DAY);
		int fileSplitHours = 24 / fileSplits;
		int part = (hh / fileSplitHours) + 1;

		return ("tweets-" + nf.format(
				year)
				+ nf.format(
						month)
				+ nf.format(
						day)
				+ "p"
				+ part
				+ ".json");
	}
}
