package mil.nga.giat.geowave.format.stanag4676;

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

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.KeyValueData;
import mil.nga.giat.geowave.format.stanag4676.image.ImageChipInfo;
import mil.nga.giat.geowave.format.stanag4676.image.ImageChipUtils;
import mil.nga.giat.geowave.format.stanag4676.parser.TrackReader.ProcessMessage;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MotionEventPoint;
import mil.nga.giat.geowave.format.stanag4676.parser.model.MotionImagery;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackEvent;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackMessage;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackPoint;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackRun;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.BaseEncoding;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.io.WKBWriter;

public class IngestMessageHandler implements
		ProcessMessage
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestMessageHandler.class);
	private final WKBWriter wkbWriter = new WKBWriter(
			3);
	private final static String DEFAULT_IMAGE_FORMAT = "jpg";
	private final List<KeyValueData<Text, TrackEventWritable>> intermediateData = new ArrayList<KeyValueData<Text, TrackEventWritable>>();

	public IngestMessageHandler() {}

	public List<KeyValueData<Text, TrackEventWritable>> getIntermediateData() {
		return intermediateData;
	}

	// Parses events sent out by 4676 parser code - each msg is a "Track" entry
	// - here we extract what we want and emit it as a value to group up in the
	// reducer
	@Override
	public void notify(
			final TrackMessage msg )
			throws IOException,
			InterruptedException {
		if ((msg != null) && (msg.getTracks() != null)) {
			for (final TrackEvent evt : msg.getTracks()) {
				if (evt.getPoints().size() > 0) {
					final String trackUuid = evt.getUuid().toString();
					String mission = evt.getMissionId();
					final String comment = evt.getComment();
					if ((mission == null) && (comment != null)) {
						mission = comment;
					}
					if (mission == null) {
						/* TODO: parse mission from filename? - can provide here */
						mission = "";
					}
					else {
						mission = mission.replaceAll(
								"Mission:",
								"").trim();
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

					int eventType = 0; // track point

					final TreeMap<Long, ImageChipInfo> timesWithImageChips = new TreeMap<Long, ImageChipInfo>();
					final List<MotionImagery> images = evt.getMotionImages();

					// keep track of the minimum image size and use that to size
					// the video
					int width = -1;
					int height = -1;
					for (final MotionImagery imagery : images) {
						try {
							final byte[] binary = BaseEncoding.base64().decode(
									imagery.getImageChip());
							final ImageInputStream stream = ImageIO.createImageInputStream(new ByteArrayInputStream(
									binary));
							final BufferedImage img = ImageIO.read(stream);
							if ((width < 0) || (img.getWidth() > width)) {
								width = img.getWidth();
							}
							if ((height < 0) || (img.getHeight() > height)) {
								height = img.getHeight();
							}
							timesWithImageChips.put(
									imagery.getTime(),
									new ImageChipInfo(
											img,
											imagery.getFrameNumber(),
											imagery.getPixelRow(),
											imagery.getPixelColumn()));
						}
						catch (final Exception e) {
							LOGGER.warn(
									"Unable to write image chip to file",
									e);
						}
					}

					for (final Entry<Long, ImageChipInfo> chipInfo : timesWithImageChips.entrySet()) {
						final BufferedImage img = chipInfo.getValue().getImage();

						final BufferedImage scaledImage = toBufferedImage(
								img.getScaledInstance(
										width,
										height,
										Image.SCALE_SMOOTH),
								BufferedImage.TYPE_3BYTE_BGR);
						chipInfo.getValue().setImage(
								scaledImage);
						try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
							ImageIO.write(
									scaledImage,
									DEFAULT_IMAGE_FORMAT,
									baos);
							baos.flush();
							chipInfo.getValue().setImageBytes(
									baos.toByteArray());
						}
						catch (final Exception e) {
							LOGGER.warn(
									"Unable to write image chip to file",
									e);
						}
					}

					for (final TrackPoint pt : evt.getPoints().values()) {
						eventType = 0; // track point

						final byte[] geometry = wkbWriter.write(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								pt.getLocation().longitude,
								pt.getLocation().latitude)));

						final String trackItemUUID = pt.getUuid().toString();
						final long timeStamp = pt.getEventTime();
						final long endTimeStamp = -1L;
						final double speed = pt.getSpeed();
						final double course = pt.getCourse();
						String trackItemClassification = "UNKNOWN";
						if ((pt.getSecurity() != null) && (pt.getSecurity().getClassification() != null)) {
							trackItemClassification = pt.getSecurity().getClassification().name();
						}
						final double latitude = pt.getLocation().latitude;
						final double longitude = pt.getLocation().longitude;
						final double elevation = pt.getLocation().elevation;
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
						final String motionEvent = "";

						final TrackEventWritable tw = new TrackEventWritable(
								eventType,
								geometry,
								imageBytes,
								mission,
								trackNumber,
								trackUuid,
								trackStatus,
								trackClassification,
								trackItemUUID,
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
								motionEvent,
								frameNumber);

						intermediateData.add(new KeyValueData<Text, TrackEventWritable>(
								new Text(
										trackUuid),
								tw));
					}

					for (final MotionEventPoint pt : evt.getMotionPoints().values()) {
						eventType = 1; // motion track point

						final byte[] geometry = wkbWriter.write(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								pt.getLocation().longitude,
								pt.getLocation().latitude)));

						final String trackItemUUID = pt.getUuid().toString();
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

						final TrackEventWritable tw = new TrackEventWritable(
								eventType,
								geometry,
								imageBytes,
								mission,
								trackNumber,
								trackUuid,
								trackStatus,
								trackClassification,
								trackItemUUID,
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
								motionEvent,
								frameNumber);

						// motion events emitted, grouped by track
						intermediateData.add(new KeyValueData<Text, TrackEventWritable>(
								new Text(
										trackUuid),
								tw));
					}
				}
			}
		}
	}

	private static BufferedImage toBufferedImage(
			final Image image,
			final int type ) {
		if (image instanceof BufferedImage) {
			return (BufferedImage) image;
		}
		return ImageChipUtils.toBufferedImage(
				image,
				type);
	}

	@Override
	public void notify(
			final TrackRun run ) {
		// TODO Auto-generated method stub
	}

	@Override
	public void initialize(
			final TrackRun run ) {
		// TODO Auto-generated method stub

	}

}
