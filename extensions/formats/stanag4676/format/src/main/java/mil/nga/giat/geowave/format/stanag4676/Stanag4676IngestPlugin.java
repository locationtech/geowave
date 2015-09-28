package mil.nga.giat.geowave.format.stanag4676;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.avro.AbstractStageWholeFileToAvro;
import mil.nga.giat.geowave.core.ingest.avro.WholeFile;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.KeyValueData;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.NullIndex;
import mil.nga.giat.geowave.format.stanag4676.image.ImageChip;
import mil.nga.giat.geowave.format.stanag4676.image.ImageChipDataAdapter;
import mil.nga.giat.geowave.format.stanag4676.parser.NATO4676Decoder;
import mil.nga.giat.geowave.format.stanag4676.parser.TrackFileReader;
import mil.nga.giat.geowave.format.stanag4676.parser.util.EarthVector;
import mil.nga.giat.geowave.format.stanag4676.parser.util.Length;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.jaitools.jts.CoordinateSequence2D;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;

public class Stanag4676IngestPlugin extends
		AbstractStageWholeFileToAvro<Object> implements
		IngestFromHdfsPlugin<WholeFile, Object>,
		LocalFileIngestPlugin<Object>
{
	private static Logger LOGGER = LoggerFactory.getLogger(Stanag4676IngestPlugin.class);
	private final Index[] supportedIndices;
	public final static Index IMAGE_CHIP_INDEX = new NullIndex(
			"IMAGERY_CHIPS");

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"xml",
			"4676"
		};
	}

	public Stanag4676IngestPlugin() {
		super();
		supportedIndices = new Index[] {
			IndexType.SPATIAL_VECTOR.createDefaultIndex(),
			IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex(),
			IMAGE_CHIP_INDEX
		};
	}

	@Override
	public void init(
			final File baseDirectory ) {}

	@Override
	public boolean supportsFile(
			final File file ) {
		// TODO: consider checking for schema compliance
		return file.length() > 0;
	}

	@Override
	public boolean isUseReducerPreferred() {
		return true;
	}

	@Override
	public IngestWithMapper<WholeFile, Object> ingestWithMapper() {
		return new IngestWithReducerImpl();
	}

	@Override
	public Index[] getSupportedIndices() {
		return supportedIndices;
	}

	@Override
	public IngestWithReducer<WholeFile, ?, ?, Object> ingestWithReducer() {
		return new IngestWithReducerImpl();
	}

	@Override
	public CloseableIterator<GeoWaveData<Object>> toGeoWaveData(
			final File file,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {
		return ingestWithMapper().toGeoWaveData(
				toAvroObjects(file)[0],
				primaryIndexId,
				globalVisibility);
	}

	@Override
	public WritableDataAdapter<Object>[] getDataAdapters(
			final String globalVisibility ) {
		return new IngestWithReducerImpl().getDataAdapters(globalVisibility);
	}

	private static class IngestWithReducerImpl implements
			IngestWithReducer<WholeFile, Text, TrackEventWritable, Object>,
			IngestWithMapper<WholeFile, Object>
	{
		private final SimpleFeatureBuilder ptBuilder;

		private final SimpleFeatureBuilder motionBuilder;

		private final SimpleFeatureBuilder trackBuilder;

		private final SimpleFeatureType pointType;

		private final SimpleFeatureType motionPointType;

		private final SimpleFeatureType trackType;

		public IngestWithReducerImpl() {
			pointType = Stanag4676Utils.createPointDataType();

			motionPointType = Stanag4676Utils.createMotionDataType();

			trackType = Stanag4676Utils.createTrackDataType();

			ptBuilder = new SimpleFeatureBuilder(
					pointType);

			motionBuilder = new SimpleFeatureBuilder(
					motionPointType);

			trackBuilder = new SimpleFeatureBuilder(
					trackType);
		}

		@Override
		public WritableDataAdapter<Object>[] getDataAdapters(
				final String globalVisibility ) {
			final FieldVisibilityHandler fieldVisiblityHandler = ((globalVisibility != null) && !globalVisibility.isEmpty()) ? new GlobalVisibilityHandler(
					globalVisibility) : null;
			return new WritableDataAdapter[] {
				new FeatureDataAdapter(
						pointType,
						fieldVisiblityHandler),
				new FeatureDataAdapter(
						motionPointType,
						fieldVisiblityHandler),
				new FeatureDataAdapter(
						trackType,
						fieldVisiblityHandler),
				new ImageChipDataAdapter(
						fieldVisiblityHandler)
			};
		}

		@Override
		public byte[] toBinary() {
			return new byte[] {};
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

		@Override
		public CloseableIterator<KeyValueData<Text, TrackEventWritable>> toIntermediateMapReduceData(
				final WholeFile input ) {
			final TrackFileReader fileReader = new TrackFileReader();
			fileReader.setDecoder(new NATO4676Decoder());
			fileReader.setStreaming(true);
			final IngestMessageHandler handler = new IngestMessageHandler();
			fileReader.setHandler(handler);
			fileReader.read(new ByteBufferBackedInputStream(
					input.getOriginalFile()));
			return new CloseableIterator.Wrapper<KeyValueData<Text, TrackEventWritable>>(
					handler.getIntermediateData().iterator());
		}

		@Override
		public CloseableIterator<GeoWaveData<Object>> toGeoWaveData(
				final Text key,
				final ByteArrayId primaryIndexId,
				final String globalVisibility,
				final Iterable<TrackEventWritable> values ) {
			final List<GeoWaveData<Object>> geowaveData = new ArrayList<GeoWaveData<Object>>();
			// sort events
			final List<TrackEventWritable> sortedTracks = new ArrayList<TrackEventWritable>();

			for (final TrackEventWritable track : values) {
				sortedTracks.add(TrackEventWritable.clone(track));
			}

			Collections.sort(
					sortedTracks,
					new ComparatorTrackEventWritable());

			// define event values
			String trackUuid = "";
			String mission = "";
			String trackNumber = "";
			final String trackStatus = "";
			String trackClassification = "";

			// initial values for track point events
			TrackEventWritable firstEvent = null;
			TrackEventWritable lastEvent = null;
			int numTrackPoints = 0;
			double distanceKm = 0.0;
			EarthVector prevEv = null;
			final ArrayList<Double> coord_sequence = new ArrayList<Double>();
			double minSpeed = Double.MAX_VALUE;
			double maxSpeed = -Double.MAX_VALUE;

			// initial values for motion events
			int numMotionPoints = 0;
			int stopCount = 0;
			int turnCount = 0;
			int uturnCount = 0;
			int stopDurationContibCount = 0;
			long stopDuration = 0L;
			long stopTime = -1L;

			for (final TrackEventWritable event : sortedTracks) {

				trackUuid = event.TrackUUID.toString();
				mission = event.Mission.toString();
				trackNumber = event.TrackNumber.toString();
				// trackStatus = event.TrackStatus.toString();
				trackClassification = event.TrackClassification.toString();

				// build collection of track point
				if (event.EventType.get() == 0) {
					// count number of track points
					numTrackPoints++;

					// grab first and last events
					if (firstEvent == null) {
						firstEvent = event;
					}
					lastEvent = event;

					final EarthVector currentEv = new EarthVector(
							EarthVector.degToRad(event.Latitude.get()),
							EarthVector.degToRad(event.Longitude.get()),
							Length.fromM(
									event.Elevation.get()).getKM());

					if (prevEv != null) {
						distanceKm += prevEv.getDistance(currentEv);
					}

					// populate coordinate sequence
					coord_sequence.add(event.Longitude.get());
					coord_sequence.add(event.Latitude.get());

					prevEv = currentEv;

					ptBuilder.add(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
							event.Longitude.get(),
							event.Latitude.get())));
					ptBuilder.add(mission);
					ptBuilder.add(trackNumber);
					ptBuilder.add(trackUuid);
					ptBuilder.add(event.TrackItemUUID.toString());
					ptBuilder.add(new Date(
							event.TimeStamp.get()));
					if (event.Speed.get() > maxSpeed) {
						maxSpeed = event.Speed.get();
					}
					if (event.Speed.get() < minSpeed) {
						minSpeed = event.Speed.get();
					}
					ptBuilder.add(new Double(
							event.Speed.get()));
					ptBuilder.add(new Double(
							event.Course.get()));
					// TODO consider more sophisticated tie between track item
					// classification and accumulo visibility
					ptBuilder.add(event.TrackItemClassification.toString());
					ptBuilder.add(new Double(
							event.Latitude.get()));
					ptBuilder.add(new Double(
							event.Longitude.get()));
					ptBuilder.add(new Double(
							event.Elevation.get()));
					ptBuilder.add(Integer.valueOf(event.FrameNumber.get()));
					ptBuilder.add(Integer.valueOf(event.PixelRow.get()));
					ptBuilder.add(Integer.valueOf(event.PixelColumn.get()));

					geowaveData.add(new GeoWaveData<Object>(
							new ByteArrayId(
									StringUtils.stringToBinary(Stanag4676Utils.TRACK_POINT)),
							primaryIndexId,
							ptBuilder.buildFeature(event.TrackItemUUID.toString())));
				}
				// build collection of motion events
				else if (event.EventType.get() == 1) {
					// count number of motion points
					numMotionPoints++;

					motionBuilder.add(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
							event.Longitude.get(),
							event.Latitude.get())));

					motionBuilder.add(mission);
					motionBuilder.add(trackNumber);
					motionBuilder.add(trackUuid);
					motionBuilder.add(event.TrackItemUUID.toString());
					motionBuilder.add(event.MotionEvent.toString());
					switch (event.MotionEvent.toString()) {
						case "STOP":
							stopCount++;
							stopTime = event.TimeStamp.get();
							break;
						case "START":
							if (stopTime > 0) {
								stopDuration += (event.TimeStamp.get() - stopTime);
								stopDurationContibCount++;
								stopTime = -1L;
							}
							break;
						case "LEFT TURN":
						case "RIGHT TURN":
							turnCount++;
							break;
						case "LEFT U TURN":
						case "RIGHT U TURN":
							uturnCount++;
							break;
						default:
					}
					motionBuilder.add(new Date(
							event.TimeStamp.get()));
					motionBuilder.add(new Date(
							event.EndTimeStamp.get()));
					// TODO consider more sophisticated tie between track item
					// classification and accumulo visibility
					motionBuilder.add(event.TrackItemClassification.toString());
					motionBuilder.add(new Double(
							event.Latitude.get()));
					motionBuilder.add(new Double(
							event.Longitude.get()));
					motionBuilder.add(new Double(
							event.Elevation.get()));
					motionBuilder.add(Integer.valueOf(event.FrameNumber.get()));
					motionBuilder.add(Integer.valueOf(event.PixelRow.get()));
					motionBuilder.add(Integer.valueOf(event.PixelColumn.get()));

					geowaveData.add(new GeoWaveData<Object>(
							new ByteArrayId(
									StringUtils.stringToBinary(Stanag4676Utils.MOTION_POINT)),
							primaryIndexId,
							motionBuilder.buildFeature(event.TrackItemUUID.toString())));
				}
				if (event.Image != null) {
					final byte[] imageBytes = event.Image.getBytes();
					if ((imageBytes != null) && (imageBytes.length > 0)) {
						geowaveData.add(new GeoWaveData(
								ImageChipDataAdapter.ADAPTER_ID,
								IMAGE_CHIP_INDEX.getId(),
								new ImageChip(
										mission,
										trackUuid,
										event.TimeStamp.get(),
										imageBytes)));
					}
				}
			}

			// create line coordinate sequence
			final Double[] xy = coord_sequence.toArray(new Double[] {});
			if ((firstEvent != null) && (lastEvent != null) && (xy.length >= 4)) {
				final CoordinateSequence2D coordinateSequence = new CoordinateSequence2D(
						ArrayUtils.toPrimitive(xy));
				final LineString lineString = GeometryUtils.GEOMETRY_FACTORY.createLineString(coordinateSequence);
				trackBuilder.add(lineString);
				trackBuilder.add(mission);
				trackBuilder.add(trackNumber);
				trackBuilder.add(trackUuid);
				trackBuilder.add(new Date(
						firstEvent.TimeStamp.get()));
				trackBuilder.add(new Date(
						lastEvent.TimeStamp.get()));
				final double durationSeconds = (lastEvent.TimeStamp.get() - firstEvent.TimeStamp.get()) / 1000.0;
				trackBuilder.add(durationSeconds);
				trackBuilder.add(minSpeed);
				trackBuilder.add(maxSpeed);
				final double distanceM = Length.fromKM(
						distanceKm).getM();
				final double avgSpeed = durationSeconds > 0 ? distanceM / durationSeconds : 0;
				trackBuilder.add(avgSpeed);
				trackBuilder.add(distanceKm);
				trackBuilder.add(new Double(
						firstEvent.Latitude.get()));
				trackBuilder.add(new Double(
						firstEvent.Longitude.get()));
				trackBuilder.add(new Double(
						lastEvent.Latitude.get()));
				trackBuilder.add(new Double(
						lastEvent.Longitude.get()));
				trackBuilder.add(numTrackPoints);
				trackBuilder.add(numMotionPoints);
				trackBuilder.add(trackStatus);
				trackBuilder.add(turnCount);
				trackBuilder.add(uturnCount);
				trackBuilder.add(stopCount);
				final double stopDurationSeconds = stopDuration / 1000.0;
				trackBuilder.add(stopDurationSeconds);
				trackBuilder.add(stopDurationContibCount > 0 ? stopDurationSeconds / stopDurationContibCount : 0.0);
				// TODO consider more sophisticated tie between track
				// classification and accumulo visibility
				trackBuilder.add(trackClassification);

				geowaveData.add(new GeoWaveData<Object>(
						new ByteArrayId(
								StringUtils.stringToBinary(Stanag4676Utils.TRACK)),
						primaryIndexId,
						trackBuilder.buildFeature(trackUuid)));

			}
			return new CloseableIterator.Wrapper<GeoWaveData<Object>>(
					geowaveData.iterator());
		}

		@Override
		public CloseableIterator<GeoWaveData<Object>> toGeoWaveData(
				final WholeFile input,
				final ByteArrayId primaryIndexId,
				final String globalVisibility ) {
			try (CloseableIterator<KeyValueData<Text, TrackEventWritable>> intermediateData = toIntermediateMapReduceData(input)) {
				// this is much better done in the reducer of a map reduce job,
				// this aggregation by track UUID is not memory efficient
				final Map<Text, List<TrackEventWritable>> trackUuidMap = new HashMap<Text, List<TrackEventWritable>>();
				while (intermediateData.hasNext()) {
					final KeyValueData<Text, TrackEventWritable> next = intermediateData.next();
					List<TrackEventWritable> trackEvents = trackUuidMap.get(next.getKey());
					if (trackEvents == null) {
						trackEvents = new ArrayList<TrackEventWritable>();
						trackUuidMap.put(
								next.getKey(),
								trackEvents);
					}
					trackEvents.add(next.getValue());
				}
				final List<CloseableIterator<GeoWaveData<Object>>> iterators = new ArrayList<CloseableIterator<GeoWaveData<Object>>>();
				for (final Entry<Text, List<TrackEventWritable>> entry : trackUuidMap.entrySet()) {
					iterators.add(toGeoWaveData(
							entry.getKey(),
							primaryIndexId,
							globalVisibility,
							entry.getValue()));
				}
				return new CloseableIterator.Wrapper<GeoWaveData<Object>>(
						Iterators.concat(iterators.iterator()));
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Error closing file '" + input.getOriginalFilePath() + "'",
						e);
			}
			return new CloseableIterator.Wrapper<GeoWaveData<Object>>(
					new ArrayList<GeoWaveData<Object>>().iterator());
		}

	}

	@Override
	public Index[] getRequiredIndices() {
		return new Index[] {
			IMAGE_CHIP_INDEX
		};
	}

	@Override
	public IngestPluginBase<WholeFile, Object> getIngestWithAvroPlugin() {
		return ingestWithMapper();
	}

}
