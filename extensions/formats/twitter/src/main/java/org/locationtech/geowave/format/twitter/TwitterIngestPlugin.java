/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.twitter;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.ingest.avro.AvroWholeFile;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.IngestPluginBase;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/*
 */
public class TwitterIngestPlugin extends AbstractSimpleFeatureIngestPlugin<AvroWholeFile> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterIngestPlugin.class);

  private final SimpleFeatureBuilder twitterSftBuilder;
  private final SimpleFeatureType twitterSft;

  public TwitterIngestPlugin() {
    twitterSft = TwitterUtils.createTwitterEventDataType();
    twitterSftBuilder = new SimpleFeatureBuilder(twitterSft);
  }

  @Override
  protected SimpleFeatureType[] getTypes() {
    return new SimpleFeatureType[] {
        SimpleFeatureUserDataConfigurationSet.configureType(twitterSft)};
  }

  @Override
  public String[] getFileExtensionFilters() {
    return new String[] {"gz"};
  }

  @Override
  public void init(final URL baseDirectory) {}

  @Override
  public boolean supportsFile(final URL file) {
    return TwitterUtils.validate(file);
  }

  @Override
  public Schema getAvroSchema() {
    return AvroWholeFile.getClassSchema();
  }

  @Override
  public CloseableIterator<AvroWholeFile> toAvroObjects(final URL input) {
    final AvroWholeFile avroFile = new AvroWholeFile();
    avroFile.setOriginalFilePath(input.getPath());
    try {
      avroFile.setOriginalFile(ByteBuffer.wrap(IOUtils.toByteArray(input)));
    } catch (final IOException e) {
      LOGGER.warn("Unable to read Twitter file: " + input.getPath(), e);
      return new CloseableIterator.Empty<>();
    }

    return new CloseableIterator.Wrapper<>(Iterators.singletonIterator(avroFile));
  }

  @Override
  public boolean isUseReducerPreferred() {
    return false;
  }

  @Override
  public IngestWithMapper<AvroWholeFile, SimpleFeature> ingestWithMapper() {
    return new IngestTwitterFromHdfs(this);
  }

  @Override
  public IngestWithReducer<AvroWholeFile, ?, ?, SimpleFeature> ingestWithReducer() {
    // unsupported right now
    throw new UnsupportedOperationException("Twitter events cannot be ingested with a reducer");
  }

  @Override
  @SuppressFBWarnings(
      value = {"REC_CATCH_EXCEPTION"},
      justification = "Intentionally catching any possible exception as there may be unknown format issues in a file and we don't want to error partially through parsing")
  protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
      final AvroWholeFile hfile,
      final String[] indexNames) {

    final List<GeoWaveData<SimpleFeature>> featureData = new ArrayList<>();

    final InputStream in = new ByteArrayInputStream(hfile.getOriginalFile().array());

    try {
      final GZIPInputStream zip = new GZIPInputStream(in);

      final InputStreamReader isr = new InputStreamReader(zip, StringUtils.UTF8_CHARSET);
      final BufferedReader br = new BufferedReader(isr);

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

      StringReader sr = new StringReader("");
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
            sr = new StringReader(line);
            jsonReader = Json.createReader(sr);
            final JsonObject tweet = jsonReader.readObject();

            try {
              lon =
                  tweet.getJsonObject("coordinates").getJsonArray("coordinates").getJsonNumber(
                      0).doubleValue();
              lat =
                  tweet.getJsonObject("coordinates").getJsonArray("coordinates").getJsonNumber(
                      1).doubleValue();
              LOGGER.debug("line " + lineNumber + " at POINT(" + lon + " " + lat + ")");
            } catch (final Exception e) {
              LOGGER.debug(
                  "Error reading twitter coordinate on line "
                      + lineNumber
                      + " of "
                      + hfile.getOriginalFilePath()
                      + "\n"
                      + line,
                  e);
              continue;
            }

            final Coordinate coord = new Coordinate(lon, lat);

            try {

              dtgString = tweet.getString("created_at");
              dtg = TwitterUtils.parseDate(dtgString);
            } catch (final Exception e) {
              LOGGER.warn(
                  "Error reading tweet date on line "
                      + lineNumber
                      + " of "
                      + hfile.getOriginalFilePath(),
                  e);
              continue;
            }

            final JsonObject user = tweet.getJsonObject("user");

            tweetId = tweet.getString("id_str");
            userid = user.getString("id_str");
            userName = user.getString("name");

            tweetText = tweet.getString("text");

            // nullable
            if (!tweet.isNull("in_reply_to_user_id_str")) {
              inReplyUser = tweet.getString("in_reply_to_user_id_str");
            }

            if (!tweet.isNull("in_reply_to_status_id_str")) {
              inReplyStatus = tweet.getString("in_reply_to_status_id_str");
            }

            retweetCount = tweet.getInt("retweet_count");

            if (!tweet.isNull("lang")) {
              lang = tweet.getString("lang");
            }

            twitterSftBuilder.set(TwitterUtils.TWITTER_USERID_ATTRIBUTE, userid);
            twitterSftBuilder.set(TwitterUtils.TWITTER_USERNAME_ATTRIBUTE, userName);
            twitterSftBuilder.set(TwitterUtils.TWITTER_TEXT_ATTRIBUTE, tweetText);
            twitterSftBuilder.set(TwitterUtils.TWITTER_INREPLYTOUSER_ATTRIBUTE, inReplyUser);
            twitterSftBuilder.set(TwitterUtils.TWITTER_INREPLYTOSTATUS_ATTRIBUTE, inReplyStatus);
            twitterSftBuilder.set(TwitterUtils.TWITTER_RETWEETCOUNT_ATTRIBUTE, retweetCount);
            twitterSftBuilder.set(TwitterUtils.TWITTER_LANG_ATTRIBUTE, lang);
            twitterSftBuilder.set(TwitterUtils.TWITTER_DTG_ATTRIBUTE, dtg);
            twitterSftBuilder.set(
                TwitterUtils.TWITTER_GEOMETRY_ATTRIBUTE,
                geometryFactory.createPoint(coord));

            final SimpleFeature tweetSft = twitterSftBuilder.buildFeature(tweetId);
            // LOGGER.warn(tweetSft.toString());

            featureData.add(new GeoWaveData<>(TwitterUtils.TWITTER_SFT_NAME, indexNames, tweetSft));
          } catch (final Exception e) {

            LOGGER.error("Error parsing line: " + line, e);
            continue;
          } finally {
            if (sr != null) {
              sr.close();
            }
            if (jsonReader != null) {
              jsonReader.close();
            }
          }
        }

      } catch (final IOException e) {
        LOGGER.warn("Error reading line from Twitter file: " + hfile.getOriginalFilePath(), e);
      } finally {
        IOUtils.closeQuietly(br);
        IOUtils.closeQuietly(isr);
        IOUtils.closeQuietly(in);
      }
    } catch (final IOException e) {
      LOGGER.error("Failed to read gz entry: " + hfile.getOriginalFilePath(), e);
    }
    return new CloseableIterator.Wrapper<>(featureData.iterator());
  }

  @Override
  public Index[] getRequiredIndices() {
    return new Index[] {};
  }


  @Override
  public String[] getSupportedIndexTypes() {
    return new String[] {SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, TimeField.DEFAULT_FIELD_ID};
  }

  @Override
  public IngestPluginBase<AvroWholeFile, SimpleFeature> getIngestWithAvroPlugin() {
    return new IngestTwitterFromHdfs(this);
  }

  public static class IngestTwitterFromHdfs extends
      AbstractIngestSimpleFeatureWithMapper<AvroWholeFile> {
    public IngestTwitterFromHdfs() {
      this(new TwitterIngestPlugin());
    }

    public IngestTwitterFromHdfs(final TwitterIngestPlugin parentPlugin) {
      super(parentPlugin);
    }
  }

}
