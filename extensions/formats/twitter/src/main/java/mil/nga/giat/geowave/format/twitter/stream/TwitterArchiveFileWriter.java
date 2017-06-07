package mil.nga.giat.geowave.format.twitter.stream;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
	private final boolean zipit;

	public TwitterArchiveFileWriter(
			final String archivePath,
			final int fileSplits,
			final boolean zipit ) {
		cal = Calendar.getInstance(
				TimeZone.getTimeZone(
						"GMT"));
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
		this.fileSplits = Math.min(
				4,
				Math.max(
						1,
						fileSplits));

		this.zipit = zipit;
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

		if (zipit) {
			writeStringToZip(
					tweetFile,
					tweetLine);
		}
		else {
			FileUtils.writeStringToFile(
					tweetFile,
					tweetLine,
					true);
		}
	}

	public void writeStringToZip(
			File tweetFile,
			String tweetLine )
			throws IOException {
		OutputStream out = null;
		GzipCompressorOutputStream cos = null;
		
		try {
			out = FileUtils.openOutputStream(
					tweetFile,
					true);

			cos = new GzipCompressorOutputStream(
					out);

			IOUtils.write(
					tweetLine,
					cos,
					Charset.defaultCharset());
		}
		finally {
			IOUtils.closeQuietly(
					cos);
			IOUtils.closeQuietly(
					out);
		}

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
					+ "p0" + ".json");
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
				+ "p" + part + ".json"
				+ (zipit ? ".gz" : ""));
	}
}
