package mil.nga.giat.geowave.format.twitter.stream;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import twitter4j.JSONObject;
import twitter4j.Status;

public class TwitterArchiveFileWriter implements TwitterArchiveWriter
{
	private final static Logger LOGGER = Logger.getLogger(TwitterArchiveFileWriter.class);

	private Calendar cal;
	private NumberFormat nf;
	private String archivePath;
	
	public TwitterArchiveFileWriter(String archivePath) {
		cal = Calendar.getInstance();
		if (archivePath == null || archivePath.contains("temp") || archivePath.contains("tmp")) {
			this.archivePath = FileUtils.getTempDirectoryPath();
		}
		else {
			this.archivePath = archivePath;
		}
		
		nf = NumberFormat.getIntegerInstance();
		nf.setMinimumIntegerDigits(2);
		nf.setGroupingUsed(false);
	}
	
	@Override
	public void writeTweet(
			final Status status,
			final JSONObject json) throws IOException {
	    
	    File tweetFile = new File(
	    		archivePath, 
	    		getArchiveFileNameForDate(status.getCreatedAt()));
	    
	    String tweetLine = json.toString() + "\n";
	    LOGGER.info(tweetLine);
		
	    FileUtils.writeStringToFile(
	    		tweetFile, 
	    		tweetLine, 
	    		true);
	}

	private String getArchiveFileNameForDate(Date date) {
		cal.setTime(date);
	    int year = cal.get(Calendar.YEAR);
	    int month = cal.get(Calendar.MONTH) + 1;
	    int day = cal.get(Calendar.DAY_OF_MONTH);
	    
	    return ("tweets-" + nf.format(year) + nf.format(month) + nf.format(day) + ".json");
	}
}
