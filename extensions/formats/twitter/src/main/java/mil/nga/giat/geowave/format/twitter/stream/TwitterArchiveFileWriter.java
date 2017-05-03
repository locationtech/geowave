package mil.nga.giat.geowave.format.twitter.stream;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;

import org.apache.commons.io.FileUtils;

import twitter4j.Status;

public class TwitterArchiveFileWriter implements TwitterArchiveWriter
{
	private Calendar cal;
	private String archivePath;
	
	public TwitterArchiveFileWriter(String archivePath) {
		cal = Calendar.getInstance();
		this.archivePath = archivePath;
	}
	
	@Override
	public void writeTweet(
			Status status ) throws IOException {
	    String statusAsString = 
	        "StatusJSONImpl{" +
	                "createdAt=" + status.getCreatedAt() +
	                ", id=" + status.getId() +
	                ", text='" + status.getText() + '\'' +
	                ", source='" + status.getSource() + '\'' +
	                ", isTruncated=" + status.isTruncated() +
	                ", inReplyToStatusId=" + status.getInReplyToStatusId() +
	                ", inReplyToUserId=" + status.getInReplyToUserId() +
	                ", isFavorited=" + status.isFavorited() +
	                ", isRetweeted=" + status.isRetweeted() +
	                ", favoriteCount=" + status.getFavoriteCount() +
	                ", inReplyToScreenName='" + status.getInReplyToScreenName() + '\'' +
	                ", geoLocation=" + status.getGeoLocation() +
	                ", place=" + status.getPlace() +
	                ", retweetCount=" + status.getRetweetCount() +
	                ", isPossiblySensitive=" + status.isPossiblySensitive() +
	                ", lang='" + status.getLang() + '\'' +
	                ", contributorsIDs=" + Arrays.toString(status.getContributors()) +
	                ", retweetedStatus=" + status.getRetweetedStatus() +
	                ", userMentionEntities=" + Arrays.toString(status.getUserMentionEntities()) +
	                ", urlEntities=" + Arrays.toString(status.getURLEntities()) +
	                ", hashtagEntities=" + Arrays.toString(status.getHashtagEntities()) +
	                ", mediaEntities=" + Arrays.toString(status.getMediaEntities()) +
	                ", symbolEntities=" + Arrays.toString(status.getSymbolEntities()) +
	                ", currentUserRetweetId=" + status.getCurrentUserRetweetId() +
	                ", user=" + status.getUser() +
	                "}";
	    
	    int year = cal.get(Calendar.YEAR);
	    int month = cal.get(Calendar.MONTH);
	    int day = cal.get(Calendar.DAY_OF_MONTH);
	    
	    String tweetFileName = "tweets-" + year + month + day;
	    File tweetFile = new File(archivePath, tweetFileName);
		
	    FileUtils.writeStringToFile(tweetFile, statusAsString, true);
	}

}
