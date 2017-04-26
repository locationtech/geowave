package mil.nga.giat.geowave.types.stanag4676.service.rest;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import javax.imageio.ImageIO;
import javax.servlet.ServletContext;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.format.stanag4676.Stanag4676IngestPlugin;
import mil.nga.giat.geowave.format.stanag4676.image.ImageChip;
import mil.nga.giat.geowave.format.stanag4676.image.ImageChipDataAdapter;
import mil.nga.giat.geowave.format.stanag4676.image.ImageChipUtils;
import mil.nga.giat.geowave.service.ServiceUtils;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jcodec.codecs.vpx.NopRateControl;
import org.jcodec.codecs.vpx.RateControl;
import org.jcodec.codecs.vpx.VP8Encoder;
import org.jcodec.common.FileChannelWrapper;
import org.jcodec.common.NIOUtils;
import org.jcodec.common.model.ColorSpace;
import org.jcodec.common.model.Picture;
import org.jcodec.common.model.Size;
import org.jcodec.containers.mkv.muxer.MKVMuxer;
import org.jcodec.containers.mkv.muxer.MKVMuxerTrack;
import org.jcodec.scale.AWTUtil;
import org.jcodec.scale.RgbToYuv420p;

import com.google.common.io.Files;

@Path("stanag4676")
public class Stanag4676ImageryChipService
{
	private static Logger LOGGER = LoggerFactory.getLogger(Stanag4676ImageryChipService.class);
	@Context
	ServletContext context;
	private static DataStore dataStore;

	// private Map<String, Object> configOptions;

	@GET
	@Path("image/{mission}/{track}/{year}-{month}-{day}T{hour}:{minute}:{second}.{millis}.jpg")
	@Produces("image/jpeg")
	public Response getImage(
			final @PathParam("mission") String mission,
			final @PathParam("track") String track,
			@PathParam("year")
			final int year,
			@PathParam("month")
			final int month,
			@PathParam("day")
			final int day,
			@PathParam("hour")
			final int hour,
			@PathParam("minute")
			final int minute,
			@PathParam("second")
			final int second,
			@PathParam("millis")
			final int millis,
			@QueryParam("size")
			@DefaultValue("-1")
			final int targetPixelSize ) {
		final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.set(
				year,
				month - 1,
				day,
				hour,
				minute,
				second);
		cal.set(
				Calendar.MILLISECOND,
				millis);
		final DataStore dataStore = getSingletonInstance();
		if (null == dataStore) {
			return Response.serverError().entity(
					"Error accessing datastore!!").build();
		}
		String chipNameStr = "mission = '" + mission + "', track = '" + track + "'";

		Object imageChip = null;
		// ImageChipUtils.getDataId(mission,track,cal.getTimeInMillis()).getBytes()
		try (CloseableIterator<Object> imageChipIt = dataStore.query(
				new QueryOptions(
						ImageChipDataAdapter.ADAPTER_ID,
						Stanag4676IngestPlugin.IMAGE_CHIP_INDEX.getId()),
				new PrefixIdQuery(
						new ByteArrayId(
								ByteArrayUtils.combineArrays(
										ImageChipDataAdapter.ADAPTER_ID.getBytes(),
										ImageChipUtils.getDataId(
												mission,
												track,
												cal.getTimeInMillis()).getBytes()))))) {

			imageChip = (imageChipIt.hasNext()) ? imageChipIt.next() : null;
		}
		catch (IOException e1) {
			LOGGER.error(
					"Unablable to find image chip for " + chipNameStr + " at " + cal,
					e1);
			return Response.serverError().entity(
					"Error generating JPEG from image chip").build();
		}

		if ((imageChip != null) && (imageChip instanceof ImageChip)) {
			if (targetPixelSize <= 0) {
				LOGGER.info("Sending ImageChip for " + chipNameStr);

				final byte[] imageData = ((ImageChip) imageChip).getImageBinary();
				return Response.ok().entity(
						imageData).type(
						"image/jpeg").build();
			}
			else {
				LOGGER.info("Sending BufferedImage for " + chipNameStr);
				final BufferedImage image = ((ImageChip) imageChip).getImage(targetPixelSize);
				final ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try {
					ImageIO.write(
							image,
							"jpeg",
							baos);

					return Response.ok().entity(
							baos.toByteArray()).type(
							"image/jpeg").build();
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to write image chip content to JPEG",
							e);
					return Response.serverError().entity(
							"Error generating JPEG from image chip for mission/track.").build();
				}
			}
		}
		return Response.serverError().entity(
				"Cannot find image chip with mission/track/time.").build();
	}

	// ------------------------------------------------------------------------------
	// ------------------------------------------------------------------------------

	@GET
	@Path("video/{mission}/{track}.webm")
	@Produces("video/webm")
	public Response getVideo(
			final @PathParam("mission") String mission,
			final @PathParam("track") String track,
			@QueryParam("size")
			@DefaultValue("-1")
			final int targetPixelSize,
			@QueryParam("speed")
			@DefaultValue("1")
			final double speed,
			@QueryParam("source")
			@DefaultValue("0")
			final double source ) {
		String videoNameStr = "mission = '" + mission + "', track = '" + track + "'" + "', source = '" + source + "'";

		final DataStore dataStore = getSingletonInstance();
		final TreeMap<Long, BufferedImage> imageChips = new TreeMap<Long, BufferedImage>();
		int width = -1;
		int height = -1;
		try (CloseableIterator<Object> imageChipIt = dataStore.query(
				new QueryOptions(
						ImageChipDataAdapter.ADAPTER_ID,
						Stanag4676IngestPlugin.IMAGE_CHIP_INDEX.getId()),
				new PrefixIdQuery(
						new ByteArrayId(
								ByteArrayUtils.combineArrays(
										ImageChipDataAdapter.ADAPTER_ID.getBytes(),
										ImageChipUtils.getTrackDataIdPrefix(
												mission,
												track).getBytes()))))) {

			while (imageChipIt.hasNext()) {
				final Object imageChipObj = imageChipIt.next();
				if ((imageChipObj != null) && (imageChipObj instanceof ImageChip)) {
					final ImageChip imageChip = (ImageChip) imageChipObj;
					final BufferedImage image = imageChip.getImage(targetPixelSize);
					if ((width < 0) || (image.getWidth() > width)) {
						width = image.getWidth();
					}
					if ((height < 0) || (image.getHeight() > height)) {
						height = image.getHeight();
					}
					imageChips.put(
							imageChip.getTimeMillis(),
							image);
				}
			}
		}
		catch (Exception e1) {
			LOGGER.error(
					"Unable to read data to compose video file",
					e1);
			return Response
					.serverError()
					.entity(
							"Video generation failed \nException: " + e1.getLocalizedMessage() + "\n stack trace: "
									+ Arrays.toString(e1.getStackTrace()))
					.build();
		}

		// ----------------------------------------------------

		if (imageChips.isEmpty()) {
			return Response.serverError().entity(
					"Unable to retrieve image chips").build();
		}
		else {
			LOGGER.info("Sending Video for " + videoNameStr);

			try {
				final File responseBody;
				LOGGER.debug("Attempting to build the video the new way ...");
				responseBody = buildVideo2(
						mission,
						track,
						imageChips,
						width,
						height,
						speed);
				LOGGER.debug("Got a response body (path): " + responseBody.getAbsolutePath());
				try (FileInputStream fis = new FileInputStream(
						responseBody) {

					@Override
					public void close()
							throws IOException {
						// super.close();
						// try to delete the file immediately after it is
						// returned

						if (!responseBody.delete()) {
							LOGGER.warn("Cannot delete response body");
						}

						if (!responseBody.getParentFile().delete()) {
							LOGGER.warn("Cannot delete response body's parent file");
						}
					}
				}) {
					LOGGER.info("Returning video object: " + fis);
					return Response.ok().entity(
							fis).type(
							"video/webm").build();
				}
				catch (final FileNotFoundException fnfe) {
					LOGGER.error(
							"Unable to find video file",
							fnfe);
					return Response.serverError().entity(
							"Video generation failed.").build();
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to write video file",
							e);
					return Response.serverError().entity(
							"Video generation failed.").build();
				}

			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to write video file",
						e);
				return Response.serverError().entity(
						"Video generation failed.").build();
			}
		}
	}

	// ------------------------------------------------------------------------------
	// ------------------------------------------------------------------------------

	// private static File buildVideo(
	// final String mission,
	// final String track,
	// final TreeMap<Long, BufferedImage> data,
	// final int width,
	// final int height,
	// final double timeScaleFactor )
	// throws IOException {
	// final File videoFileDir = Files.createTempDir();
	// LOGGER.info("Write to tempfile: " + videoFileDir.getAbsolutePath());
	// videoFileDir.deleteOnExit();
	// final File videoFile = new File(
	// videoFileDir,
	// mission + "_" + track + ".webm");
	// videoFile.deleteOnExit();
	// final IMediaWriter writer =
	// ToolFactory.makeWriter(videoFile.getAbsolutePath());
	// writer.addVideoStream(
	// 0,
	// 0,
	// ICodec.ID.CODEC_ID_VP8,
	// width,
	// height);
	// final Long startTime = data.firstKey();
	//
	// final double timeNormalizationFactor = 1.0 / timeScaleFactor;
	//
	// int i = 0;
	// int y = 0;
	// for (final Entry<Long, BufferedImage> e : data.entrySet()) {
	// if ((e.getValue().getWidth() == width) && (e.getValue().getHeight() ==
	// height)) {
	// writer.encodeVideo(
	// 0,
	// e.getValue(),
	// (long) ((e.getKey() - startTime) * timeNormalizationFactor),
	// TimeUnit.MILLISECONDS);
	// ++y;
	// }
	// ++i;
	// }
	// writer.close();
	// LOGGER.error("Found " + y + " of " + i + " old fashioned frames");
	//
	// return videoFile;
	// }

	// ------------------------------------------------------------------------------
	// ------------------------------------------------------------------------------

	private static final int MAX_FRAMES = 2000;

	private static File buildVideo2(
			final String mission,
			final String track,
			final TreeMap<Long, BufferedImage> data,
			final int width,
			final int height,
			final double timeScaleFactor )
			throws IOException {

		final File videoFileDir = Files.createTempDir();
		final File videoFile = new File(
				videoFileDir,
				mission + "_" + track + ".webm");

		FileChannelWrapper sink = null;

		try {
			sink = NIOUtils.writableFileChannel(videoFile.getAbsolutePath());

			/*
			 * Version 0.1.9
			 */
			RateControl rc = new NopRateControl(
					10);
			// (int) timeScaleFactor);

			VP8Encoder encoder = new VP8Encoder(
					rc); // qp
			RgbToYuv420p transform = new RgbToYuv420p(
					0,
					0);

			MKVMuxer muxer = new MKVMuxer();
			MKVMuxerTrack videoTrack = null;

			int i = 0;
			int y = 0;
			for (final Entry<Long, BufferedImage> e : data.entrySet()) {
				BufferedImage rgb = e.getValue();

				if (videoTrack == null) {
					videoTrack = muxer.createVideoTrack(
							new Size(
									rgb.getWidth(),
									rgb.getHeight()),
							"V_VP8");
				}
				Picture yuv = Picture.create(
						rgb.getWidth(),
						rgb.getHeight(),
						ColorSpace.YUV420);
				transform.transform(
						AWTUtil.fromBufferedImage(rgb),
						yuv);
				ByteBuffer buf = ByteBuffer.allocate(rgb.getWidth() * rgb.getHeight() * 3);

				ByteBuffer ff = encoder.encodeFrame(
						yuv,
						buf);

				// Frame number must be from 1 to ...
				videoTrack.addSampleEntry(
						ff,
						(int) (i * timeScaleFactor) + 1);

				++y;
				if ((++i) > MAX_FRAMES) {
					break;
				}
			}
			if (i == 1) {
				LOGGER.error("Image sequence not found");
				return null;
			}
			if (videoTrack != null) {
				LOGGER.debug("Found " + y + " of " + i + " new frames." + "  videoTrack timescale is "
						+ videoTrack.getTimescale());
			}
			muxer.mux(sink);

			// ------------------------------------------------------------------
			// Version 0.2.0
			// ------------------------------------------------------------------

			// VP8Encoder encoder = VP8Encoder.createVP8Encoder((int)
			// timeScaleFactor); // qp
			// RgbToYuv420p8Bit transform = new RgbToYuv420p8Bit();
			// final Long startTime = data.firstKey();
			// final double timeNormalizationFactor = 1.0 / timeScaleFactor;
			//
			// MKVMuxer muxer = new MKVMuxer();
			// MKVMuxerTrack videoTrack = null;
			//
			// /*
			// * writer.encodeVideo( 0, frame_data, (long) ((e.getKey() -
			// * startTime) * timeNormalizationFactor), TimeUnit.MILLISECONDS);
			// */
			//
			// int i = 0;
			// for (final Entry<Long, BufferedImage> e : data.entrySet()) {
			// BufferedImage rgb = e.getValue();
			// if (videoTrack == null) {
			// videoTrack =
			// muxer.createVideoTrack(
			// new Size(rgb.getWidth(), rgb.getHeight()), "V_VP8"); }
			// Picture8Bit yuv =
			// Picture8Bit.create(rgb.getWidth(), rgb.getHeight(),
			// ColorSpace.YUV420);
			//
			// transform.transform(AWTUtil.fromBufferedImageRGB8Bit(rgb), yuv);
			// ByteBuffer buf = ByteBuffer.allocate(rgb.getWidth() *
			// rgb.getHeight() * 3);
			// ByteBuffer ff = encoder.encodeFrame8Bit(yuv, buf);
			//
			// videoTrack.addSampleEntry(ff, i - 1);
			// if ((++i) > MAX_FRAMES) {
			// break;
			// }
			// }
			// if (i == 1) {
			// System.out.println("Image sequence not found"); return null;
			// }
			// muxer.mux(sink);

			// ------------------------------------------------------------------

		}
		finally {
			if (sink != null) {
				sink.close();
				IOUtils.closeQuietly(sink);
			}
		}
		return videoFile;
	}

	// ------------------------------------------------------------------------------
	// ------------------------------------------------------------------------------

	private synchronized DataStore getSingletonInstance() {
		if (dataStore != null) {
			return dataStore;
		}
		String confPropFilename = context.getInitParameter("config.properties");
		// HP Fortify "Log Forging" false positive
		// What Fortify considers "user input" comes only
		// from users with OS-level access anyway
		LOGGER.info("Creating datastore singleton for 4676 service.   conf prop filename: " + confPropFilename);
		Properties props = null;
		try (InputStream is = context.getResourceAsStream(confPropFilename)) {
			props = ServiceUtils.loadProperties(is);
		}
		catch (IOException e) {
			LOGGER.error(
					e.getLocalizedMessage(),
					e);
		}
		LOGGER.info(
				"Found {} props",
				(props != null ? props.size() : 0));
		if (props != null) {
			final Map<String, String> strMap = new HashMap<String, String>();

			final Set<Object> keySet = props.keySet();
			final Iterator<Object> it = keySet.iterator();
			while (it.hasNext()) {
				final String key = it.next().toString();
				final String value = ServiceUtils.getProperty(
						props,
						key);
				strMap.put(
						key,
						value);
				// HP Fortify "Log Forging" false positive
				// What Fortify considers "user input" comes only
				// from users with OS-level access anyway
				LOGGER.info("    Key/Value: " + key + "/" + value);
			}
			// Can be removed when factory is fixed
			GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies();

			dataStore = GeoWaveStoreFinder.createDataStore(strMap);

		}
		if (dataStore == null) {
			LOGGER.error("Unable to create datastore for 4676 service");
		}
		return dataStore;
	}
}
