/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;
import org.apache.hadoop.io.Text;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.KeyValueData;
import org.locationtech.geowave.format.stanag4676.image.ImageChipInfo;
import org.locationtech.geowave.format.stanag4676.image.ImageChipUtils;
import org.locationtech.geowave.format.stanag4676.parser.TrackReader.ProcessMessage;
import org.locationtech.geowave.format.stanag4676.parser.model.MissionFrame;
import org.locationtech.geowave.format.stanag4676.parser.model.MissionSummary;
import org.locationtech.geowave.format.stanag4676.parser.model.MissionSummaryMessage;
import org.locationtech.geowave.format.stanag4676.parser.model.ModalityType;
import org.locationtech.geowave.format.stanag4676.parser.model.MotionEventPoint;
import org.locationtech.geowave.format.stanag4676.parser.model.MotionImagery;
import org.locationtech.geowave.format.stanag4676.parser.model.NATO4676Message;
import org.locationtech.geowave.format.stanag4676.parser.model.ObjectClassification;
import org.locationtech.geowave.format.stanag4676.parser.model.TrackClassification;
import org.locationtech.geowave.format.stanag4676.parser.model.TrackEvent;
import org.locationtech.geowave.format.stanag4676.parser.model.TrackMessage;
import org.locationtech.geowave.format.stanag4676.parser.model.TrackPoint;
import org.locationtech.geowave.format.stanag4676.parser.model.TrackRun;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.io.BaseEncoding;

public class IngestMessageHandler implements ProcessMessage {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestMessageHandler.class);
  private final WKBWriter wkbWriter = new WKBWriter(3);
  private static final String DEFAULT_IMAGE_FORMAT = "jpg";
  private final List<KeyValueData<Text, Stanag4676EventWritable>> intermediateData =
      new ArrayList<>();

  public IngestMessageHandler() {}

  public List<KeyValueData<Text, Stanag4676EventWritable>> getIntermediateData() {
    return intermediateData;
  }

  // Parses events sent out by 4676 parser code - each msg is a "Track" entry
  // - here we extract what we want and emit it as a value to group up in the
  // reducer
  @Override
  public void notify(final NATO4676Message msg) throws IOException, InterruptedException {

    if (msg == null) {
      LOGGER.error("Received null msg");
      return;
    }

    if (msg instanceof TrackMessage) {
      final TrackMessage trackMessage = (TrackMessage) msg;
      for (final TrackEvent evt : trackMessage.getTracks()) {
        if (evt.getPoints().size() > 0) {
          final String trackUuid = evt.getUuid().toString();
          String missionUUID = evt.getMissionId();
          final String comment = evt.getComment();
          if ((missionUUID == null) && (comment != null)) {
            missionUUID = comment;
          }
          if (missionUUID == null) {
            /* TODO: parse mission from filename? - can provide here */
            missionUUID = "";
          } else {
            missionUUID = missionUUID.replaceAll("Mission:", "").trim();
          }
          final String trackNumber = evt.getTrackNumber();

          String trackStatus = "";
          if (evt.getStatus() != null) {
            trackStatus = evt.getStatus().name();
          }

          String trackClassification = "";
          if ((evt.getSecurity() != null) && (evt.getSecurity().getClassification() != null)) {
            trackClassification = evt.getSecurity().getClassification().name();
          }

          final TreeMap<Long, ImageChipInfo> timesWithImageChips = new TreeMap<>();
          final List<MotionImagery> images = evt.getMotionImages();

          // keep track of the minimum image size and use that to size
          // the video
          int width = -1;
          int height = -1;
          for (final MotionImagery imagery : images) {
            try {
              final String imageChip = imagery.getImageChip();
              BufferedImage img = null;
              if ((imageChip != null) && (imageChip.length() > 0)) {
                final byte[] binary = BaseEncoding.base64().decode(imageChip);
                final ImageInputStream stream =
                    ImageIO.createImageInputStream(new ByteArrayInputStream(binary));
                img = ImageIO.read(stream);
                if ((width < 0) || (img.getWidth() > width)) {
                  width = img.getWidth();
                }
                if ((height < 0) || (img.getHeight() > height)) {
                  height = img.getHeight();
                }
              }
              timesWithImageChips.put(
                  imagery.getTime(),
                  new ImageChipInfo(
                      img,
                      imagery.getFrameNumber(),
                      imagery.getPixelRow(),
                      imagery.getPixelColumn()));
            } catch (final Exception e) {
              LOGGER.warn("Unable to write image chip to file", e);
            }
          }

          for (final Entry<Long, ImageChipInfo> chipInfo : timesWithImageChips.entrySet()) {
            final BufferedImage img = chipInfo.getValue().getImage();
            if (img != null) {
              final BufferedImage scaledImage =
                  toBufferedImage(
                      img.getScaledInstance(width, height, Image.SCALE_SMOOTH),
                      BufferedImage.TYPE_3BYTE_BGR);
              chipInfo.getValue().setImage(scaledImage);
              try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                ImageIO.write(scaledImage, DEFAULT_IMAGE_FORMAT, baos);
                baos.flush();
                chipInfo.getValue().setImageBytes(baos.toByteArray());
              } catch (final Exception e) {
                LOGGER.warn("Unable to write image chip to file", e);
              }
            }
          }

          for (final TrackPoint pt : evt.getPoints().values()) {
            final String trackItemUUID = pt.getUuid();
            final long timeStamp = pt.getEventTime();
            final long endTimeStamp = -1L;
            final double speed = pt.getSpeed();
            final double course = pt.getCourse();
            String trackItemClassification = "UNKNOWN";
            if ((pt.getSecurity() != null) && (pt.getSecurity().getClassification() != null)) {
              trackItemClassification = pt.getSecurity().getClassification().name();
            }
            final ModalityType mt = pt.getTrackPointSource();
            final String trackPointSource = (mt != null) ? mt.toString() : "";
            final double latitude = pt.getLocation().latitude;
            final double longitude = pt.getLocation().longitude;
            final double elevation = pt.getLocation().elevation;

            final byte[] geometry =
                wkbWriter.write(
                    GeometryUtils.GEOMETRY_FACTORY.createPoint(
                        new Coordinate(longitude, latitude)));

            double detailLatitude = Stanag4676EventWritable.NO_DETAIL;
            double detailLongitude = Stanag4676EventWritable.NO_DETAIL;
            double detailElevation = Stanag4676EventWritable.NO_DETAIL;
            byte[] detailGeometry = null;
            if ((pt.getDetail() != null) && (pt.getDetail().getLocation() != null)) {
              detailLatitude = pt.getDetail().getLocation().latitude;
              detailLongitude = pt.getDetail().getLocation().longitude;
              detailElevation = pt.getDetail().getLocation().elevation;
              detailGeometry =
                  wkbWriter.write(
                      GeometryUtils.GEOMETRY_FACTORY.createPoint(
                          new Coordinate(detailLongitude, detailLatitude)));
            }

            final ImageChipInfo chipInfo = timesWithImageChips.get(timeStamp);
            int pixelRow = -1;
            int pixelColumn = -1;
            int frameNumber = -1;
            byte[] imageBytes = new byte[] {};
            if (chipInfo != null) {
              pixelRow = chipInfo.getPixelRow();
              pixelColumn = chipInfo.getPixelColumn();
              frameNumber = chipInfo.getFrameNumber();
              imageBytes = chipInfo.getImageBytes();
            }
            final Stanag4676EventWritable sw = new Stanag4676EventWritable();
            sw.setTrackPointData(
                geometry,
                detailGeometry,
                imageBytes,
                missionUUID,
                trackNumber,
                trackUuid,
                trackStatus,
                trackClassification,
                trackItemUUID,
                trackPointSource,
                timeStamp,
                endTimeStamp,
                speed,
                course,
                trackItemClassification,
                latitude,
                longitude,
                elevation,
                detailLatitude,
                detailLongitude,
                detailElevation,
                pixelRow,
                pixelColumn,
                frameNumber);

            intermediateData.add(new KeyValueData<>(new Text(trackUuid), sw));
          }

          for (final MotionEventPoint pt : evt.getMotionPoints().values()) {
            final byte[] geometry =
                wkbWriter.write(
                    GeometryUtils.GEOMETRY_FACTORY.createPoint(
                        new Coordinate(pt.getLocation().longitude, pt.getLocation().latitude)));
            final String trackItemUUID = pt.getUuid();
            final long timeStamp = pt.getEventTime();
            final long endTimeStamp = pt.getEndTime();
            final double speed = pt.getSpeed();
            final double course = pt.getCourse();
            String trackItemClassification = "UNKNOWN";
            if ((pt.getSecurity() != null) && (pt.getSecurity().getClassification() != null)) {
              trackItemClassification = pt.getSecurity().getClassification().name();
            }
            final double latitude = pt.getLocation().latitude;
            final double longitude = pt.getLocation().longitude;
            final double elevation = pt.getLocation().elevation;
            final ModalityType mt = pt.getTrackPointSource();
            final String trackPointSource = (mt != null) ? mt.toString() : "";
            final ImageChipInfo chipInfo = timesWithImageChips.get(timeStamp);
            int pixelRow = -1;
            int pixelColumn = -1;
            int frameNumber = -1;
            byte[] imageBytes = new byte[] {};
            if (chipInfo != null) {
              pixelRow = chipInfo.getPixelRow();
              pixelColumn = chipInfo.getPixelColumn();
              frameNumber = chipInfo.getFrameNumber();
              imageBytes = chipInfo.getImageBytes();
            }
            final String motionEvent = pt.motionEvent;

            final Stanag4676EventWritable sw = new Stanag4676EventWritable();
            sw.setMotionPointData(
                geometry,
                imageBytes,
                missionUUID,
                trackNumber,
                trackUuid,
                trackStatus,
                trackClassification,
                trackItemUUID,
                trackPointSource,
                timeStamp,
                endTimeStamp,
                speed,
                course,
                trackItemClassification,
                latitude,
                longitude,
                elevation,
                pixelRow,
                pixelColumn,
                frameNumber,
                motionEvent);

            // motion events emitted, grouped by track
            intermediateData.add(new KeyValueData<>(new Text(trackUuid), sw));
          }

          for (final TrackClassification tc : evt.getClassifications()) {
            final long objectClassTime = tc.getTime();
            final String objectClass = tc.classification.toString();
            final int objectClassConf = tc.credibility.getValueConfidence();
            final int objectClassRel = tc.credibility.getSourceReliability();

            final Stanag4676EventWritable sw = new Stanag4676EventWritable();
            sw.setTrackObjectClassData(
                objectClassTime,
                objectClass,
                objectClassConf,
                objectClassRel);

            intermediateData.add(new KeyValueData<>(new Text(trackUuid), sw));
          }
        }
      }
    }

    if (msg instanceof MissionSummaryMessage) {
      final MissionSummaryMessage missionSummaryMessage = (MissionSummaryMessage) msg;
      final MissionSummary missionSummary = missionSummaryMessage.getMissionSummary();
      if ((missionSummary != null) && (missionSummary.getCoverageArea() != null)) {
        final Polygon missionPolygon = missionSummary.getCoverageArea().getPolygon();
        final byte[] missionGeometry = wkbWriter.write(missionPolygon);
        final String missionUUID = missionSummary.getMissionId();
        final String missionName = missionSummary.getName();
        final int missionNumFrames = missionSummary.getFrames().size();
        final long missionStartTime = missionSummary.getStartTime();
        final long missionEndTime = missionSummary.getEndTime();
        final String missionClassification = missionSummary.getSecurity();
        final StringBuilder sb = new StringBuilder();
        for (final ObjectClassification oc : missionSummary.getClassifications()) {
          if (sb.length() > 0) {
            sb.append(",");
          }
          sb.append(oc.toString());
        }
        final String activeObjectClass = sb.toString();
        final Stanag4676EventWritable msw = new Stanag4676EventWritable();
        msw.setMissionSummaryData(
            missionGeometry,
            missionUUID,
            missionName,
            missionNumFrames,
            missionStartTime,
            missionEndTime,
            missionClassification,
            activeObjectClass);

        intermediateData.add(new KeyValueData<>(new Text(missionUUID), msw));

        for (final MissionFrame frame : missionSummary.getFrames()) {
          if ((frame != null) && (frame.getCoverageArea() != null)) {
            final Polygon framePolygon = frame.getCoverageArea().getPolygon();
            final byte[] frameGeometry = wkbWriter.write(framePolygon);
            final long frameTimeStamp = frame.getFrameTime();
            final int frameNumber = frame.getFrameNumber();
            final Stanag4676EventWritable fsw = new Stanag4676EventWritable();
            fsw.setMissionFrameData(frameGeometry, missionUUID, frameNumber, frameTimeStamp);
            intermediateData.add(new KeyValueData<>(new Text(missionUUID), fsw));
          }
        }
      }
    }
  }

  private static BufferedImage toBufferedImage(final Image image, final int type) {
    if (image instanceof BufferedImage) {
      return (BufferedImage) image;
    }
    return ImageChipUtils.toBufferedImage(image, type);
  }

  @Override
  public void notify(final TrackRun run) {
    // TODO Auto-generated method stub
  }

  @Override
  public void initialize(final TrackRun run) {
    // TODO Auto-generated method stub

  }
}
