package mil.nga.giat.geowave.format.twitter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.avro.WholeFile;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

/*
 */
public class TwitterIngestPlugin extends
		AbstractSimpleFeatureIngestPlugin<WholeFile>
{

	private final static Logger LOGGER = LoggerFactory.getLogger(TwitterIngestPlugin.class);

	private SimpleFeatureBuilder twitterSftBuilder;
	private SimpleFeatureType twitterSft;

	private final ByteArrayId sftNameKey;

	public TwitterIngestPlugin() {
		twitterSft = TwitterUtils.createTwitterEventDataType();
		twitterSftBuilder = new SimpleFeatureBuilder(
				twitterSft);

		sftNameKey = new ByteArrayId(
				StringUtils.stringToBinary(TwitterUtils.TWITTER_SFT_NAME));
	}

	@Override
	protected SimpleFeatureType[] getTypes() {
		return new SimpleFeatureType[] {
			SimpleFeatureUserDataConfigurationSet.configureType(twitterSft)
		};
	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"gz"
		};
	}

	@Override
	public void init(
			final File baseDirectory ) {

	}

	@Override
	public boolean supportsFile(
			final File file ) {
		return TwitterUtils.validate(file);
	}

	@Override
	public Schema getAvroSchema() {
		return WholeFile.getClassSchema();
	}

	@Override
	public WholeFile[] toAvroObjects(
			final File input ) {
		final WholeFile avroFile = new WholeFile();
		avroFile.setOriginalFilePath(input.getAbsolutePath());
		try {
			avroFile.setOriginalFile(ByteBuffer.wrap(Files.readAllBytes(input.toPath())));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read Twitter file: " + input.getAbsolutePath(),
					e);
			return new WholeFile[] {};
		}

		return new WholeFile[] {
			avroFile
		};
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<WholeFile, SimpleFeature> ingestWithMapper() {
		return new IngestTwitterFromHdfs(
				this);
	}

	@Override
	public IngestWithReducer<WholeFile, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"Twitter events cannot be ingested with a reducer");
	}

	@Override
	@SuppressFBWarnings(value = {
		"REC_CATCH_EXCEPTION"
	}, justification = "Intentionally catching any possible exception as there may be unknown format issues in a file and we don't want to error partially through parsing")
	protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final WholeFile hfile,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility ) {

		final List<GeoWaveData<SimpleFeature>> featureData = new ArrayList<GeoWaveData<SimpleFeature>>();

		final InputStream in = new ByteArrayInputStream(
				hfile.getOriginalFile().array());

		try {
			final GZIPInputStream zip = new GZIPInputStream(
					in);

			final InputStreamReader isr = new InputStreamReader(
					zip,
					StringUtils.UTF8_CHAR_SET);
			final BufferedReader br = new BufferedReader(
					isr);

			final GeometryFactory geometryFactory = new GeometryFactory();

			String line;
			int lineNumber = 0;
			String userid = "";
			String userName = "";
			String tweetText = "";
			String inReplyUser = "";
			String inReplyStatus = "";
			int retweetCount = 0;
			String lang = "";
			Date dtg = null;
			String dtgString = "";
			String tweetId = "";
			double lat = 0;
			double lon = 0;

			StringReader sr = new StringReader(
					"");
			JsonReader jsonReader = null;

			try {
				while ((line = br.readLine()) != null) {
					userid = "";
					userName = "";
					tweetText = "";
					inReplyUser = "";
					inReplyStatus = "";
					retweetCount = 0;
					lang = "";
					dtg = null;
					dtgString = "";
					tweetId = "";
					lat = 0;
					lon = 0;

					lineNumber++;
					try {
						sr = new StringReader(
								line);
						jsonReader = Json.createReader(sr);
						JsonObject tweet = jsonReader.readObject();

						try {
							lon = tweet.getJsonObject(
									"coordinates").getJsonArray(
									"coordinates").getJsonNumber(
									0).doubleValue();
							lat = tweet.getJsonObject(
									"coordinates").getJsonArray(
									"coordinates").getJsonNumber(
									1).doubleValue();
							LOGGER.debug("line " + lineNumber + " at POINT(" + lon + " " + lat + ")");
						}
						catch (final Exception e) {
							LOGGER.debug(
									"Error reading twitter coordinate on line " + lineNumber + " of "
											+ hfile.getOriginalFilePath() + "\n" + line,
									e);
							continue;
						}

						final Coordinate coord = new Coordinate(
								lon,
								lat);

						try {

							dtgString = tweet.getString("created_at");
							dtg = TwitterUtils.parseDate(dtgString);
						}
						catch (final Exception e) {
							LOGGER.warn(
									"Error reading tweet date on line " + lineNumber + " of "
											+ hfile.getOriginalFilePath(),
									e);
							continue;
						}

						JsonObject user = tweet.getJsonObject("user");

						tweetId = tweet.getString("id_str");
						userid = user.getString("id_str");
						userName = user.getString("name");

						tweetText = tweet.getString("text");

						// nullable
						if (!tweet.isNull("in_reply_to_user_id_str"))
							inReplyUser = tweet.getString("in_reply_to_user_id_str");

						if (!tweet.isNull("in_reply_to_status_id_str"))
							inReplyStatus = tweet.getString("in_reply_to_status_id_str");

						retweetCount = tweet.getInt("retweet_count");

						if (!tweet.isNull("lang")) lang = tweet.getString("lang");

						twitterSftBuilder.set(
								TwitterUtils.TWITTER_USERID_ATTRIBUTE,
								userid);
						twitterSftBuilder.set(
								TwitterUtils.TWITTER_USERNAME_ATTRIBUTE,
								userName);
						twitterSftBuilder.set(
								TwitterUtils.TWITTER_TEXT_ATTRIBUTE,
								tweetText);
						twitterSftBuilder.set(
								TwitterUtils.TWITTER_INREPLYTOUSER_ATTRIBUTE,
								inReplyUser);
						twitterSftBuilder.set(
								TwitterUtils.TWITTER_INREPLYTOSTATUS_ATTRIBUTE,
								inReplyStatus);
						twitterSftBuilder.set(
								TwitterUtils.TWITTER_RETWEETCOUNT_ATTRIBUTE,
								retweetCount);
						twitterSftBuilder.set(
								TwitterUtils.TWITTER_LANG_ATTRIBUTE,
								lang);
						twitterSftBuilder.set(
								TwitterUtils.TWITTER_DTG_ATTRIBUTE,
								dtg);
						twitterSftBuilder.set(
								TwitterUtils.TWITTER_GEOMETRY_ATTRIBUTE,
								geometryFactory.createPoint(coord));

						SimpleFeature tweetSft = twitterSftBuilder.buildFeature(tweetId);
						// LOGGER.warn(tweetSft.toString());

						featureData.add(new GeoWaveData<SimpleFeature>(
								sftNameKey,
								primaryIndexIds,
								tweetSft));
					}
					catch (final Exception e) {

						LOGGER.error(
								"Error parsing line: " + line,
								e);
						continue;
					}
					finally {
						if (sr != null) sr.close();
						if (jsonReader != null) jsonReader.close();
					}
				}

			}
			catch (final IOException e) {
				LOGGER.warn(
						"Error reading line from Twitter file: " + hfile.getOriginalFilePath(),
						e);
			}
			finally {
				IOUtils.closeQuietly(br);
				IOUtils.closeQuietly(isr);
				IOUtils.closeQuietly(in);
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Failed to read gz entry: " + hfile.getOriginalFilePath(),
					e);
		}
		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(
				featureData.iterator());
	}

	@Override
	public PrimaryIndex[] getRequiredIndices() {
		return new PrimaryIndex[] {};
	}

	@Override
	public IngestPluginBase<WholeFile, SimpleFeature> getIngestWithAvroPlugin() {
		return new IngestTwitterFromHdfs(
				this);
	}

	public static class IngestTwitterFromHdfs extends
			AbstractIngestSimpleFeatureWithMapper<WholeFile>
	{
		public IngestTwitterFromHdfs() {
			this(
					new TwitterIngestPlugin());
		}

		public IngestTwitterFromHdfs(
				final TwitterIngestPlugin parentPlugin ) {
			super(
					parentPlugin);
		}
	}

	@Override
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}
}
