package mil.nga.giat.geowave.types.tdrive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.store.data.field.GlobalVisibilityHandler;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.mortbay.log.Log;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Coordinate;

/*
 */
public class TdriveIngestPlugin implements
		LocalFileIngestPlugin<SimpleFeature>,
		IngestFromHdfsPlugin<TdrivePoint, SimpleFeature>,
		StageToHdfsPlugin<TdrivePoint>
{

	private final static Logger LOGGER = Logger.getLogger(TdriveIngestPlugin.class);

	private final SimpleFeatureBuilder tdrivepointBuilder;
	private final SimpleFeatureType tdrivepointType;

	private final ByteArrayId pointKey;

	private final Index[] supportedIndices;

	public TdriveIngestPlugin() {

		tdrivepointType = TdriveUtils.createTdrivePointDataType();

		pointKey = new ByteArrayId(
				StringUtils.stringToBinary(TdriveUtils.TDRIVE_POINT_FEATURE));
		tdrivepointBuilder = new SimpleFeatureBuilder(
				tdrivepointType);
		supportedIndices = new Index[] {
			IndexType.SPATIAL.createDefaultIndex(),
			IndexType.SPATIAL_TEMPORAL.createDefaultIndex()
		};

	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"csv"
		};
	}

	@Override
	public void init(
			final File baseDirectory ) {

	}

	@Override
	public boolean supportsFile(
			final File file ) {
		return TdriveUtils.validate(file);
	}

	@Override
	public Index[] getSupportedIndices() {
		return supportedIndices;
	}

	@Override
	public WritableDataAdapter<SimpleFeature>[] getDataAdapters(
			final String globalVisibility ) {
		final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler = ((globalVisibility != null) && !globalVisibility.isEmpty()) ? new GlobalVisibilityHandler<SimpleFeature, Object>(
				globalVisibility) : null;
		return new WritableDataAdapter[] {
			new FeatureDataAdapter(
					tdrivepointType,
					fieldVisiblityHandler)
		};
	}

	@Override
	public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
			final File input,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {
		final TdrivePoint[] tdrivePoints = toHdfsObjects(input);
		final List<CloseableIterator<GeoWaveData<SimpleFeature>>> allData = new ArrayList<CloseableIterator<GeoWaveData<SimpleFeature>>>();
		for (final TdrivePoint track : tdrivePoints) {
			final CloseableIterator<GeoWaveData<SimpleFeature>> geowaveData = toGeoWaveDataInternal(
					track,
					primaryIndexId,
					globalVisibility);
			allData.add(geowaveData);
		}
		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(
				Iterators.concat(allData.iterator()));
	}

	@Override
	public Schema getAvroSchemaForHdfsType() {
		return TdrivePoint.getClassSchema();
	}

	@Override
	public TdrivePoint[] toHdfsObjects(
			final File input ) {
		FileReader fr = null;
		BufferedReader br = null;
		long pointInstance = 0l;
		final List<TdrivePoint> pts = new ArrayList<TdrivePoint>();
		try {
			fr = new FileReader(
					input);
			br = new BufferedReader(
					fr);
			String line;
			try {
				while ((line = br.readLine()) != null) {

					final String[] vals = line.split(",");
					final TdrivePoint td = new TdrivePoint();
					td.setTaxiid(Integer.parseInt(vals[0]));
					try {
						td.setTimestamp(TdriveUtils.TIME_FORMAT_SECONDS.parse(
								vals[1]).getTime());
					}
					catch (final ParseException e) {
						td.setTimestamp(0l);
						LOGGER.warn(
								"Couldn't parse time format: " + vals[1],
								e);
					}
					td.setLongitude(Double.parseDouble(vals[2]));
					td.setLatitude(Double.parseDouble(vals[3]));
					td.setPointinstance(pointInstance);
					pts.add(td);
					pointInstance++;
				}
			}
			catch (final IOException e) {
				Log.warn(
						"Error reading line from file: " + input.getName(),
						e);
			}
		}
		catch (final FileNotFoundException e) {
			Log.warn(
					"Error parsing tdrive file: " + input.getName(),
					e);
		}
		finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(fr);
		}
		return pts.toArray(new TdrivePoint[pts.size()]);
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<TdrivePoint, SimpleFeature> ingestWithMapper() {
		return new IngestGpxTrackFromHdfs(
				this);
	}

	@Override
	public IngestWithReducer<TdrivePoint, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GPX tracks cannot be ingested with a reducer");
	}

	private CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final TdrivePoint tdrivePoint,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {

		final List<GeoWaveData<SimpleFeature>> featureData = new ArrayList<GeoWaveData<SimpleFeature>>();

		// tdrivepointBuilder = new SimpleFeatureBuilder(tdrivepointType);
		tdrivepointBuilder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						tdrivePoint.getLongitude(),
						tdrivePoint.getLatitude())));
		tdrivepointBuilder.set(
				"taxiid",
				tdrivePoint.getTaxiid());
		tdrivepointBuilder.set(
				"pointinstance",
				tdrivePoint.getPointinstance());
		tdrivepointBuilder.set(
				"Timestamp",
				new Date(
						tdrivePoint.getTimestamp()));
		tdrivepointBuilder.set(
				"Latsitude",
				tdrivePoint.getLatitude());
		tdrivepointBuilder.set(
				"Longitude",
				tdrivePoint.getLongitude());
		featureData.add(new GeoWaveData<SimpleFeature>(
				pointKey,
				primaryIndexId,
				tdrivepointBuilder.buildFeature(tdrivePoint.getTaxiid() + "_" + tdrivePoint.getPointinstance())));

		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(
				featureData.iterator());
	}

	@Override
	public Index[] getRequiredIndices() {
		return new Index[] {};
	}

	public static class IngestGpxTrackFromHdfs implements
			IngestWithMapper<TdrivePoint, SimpleFeature>
	{
		private final TdriveIngestPlugin parentPlugin;

		public IngestGpxTrackFromHdfs() {
			this(
					new TdriveIngestPlugin());
			// this constructor will be used when deserialized
		}

		public IngestGpxTrackFromHdfs(
				final TdriveIngestPlugin parentPlugin ) {
			this.parentPlugin = parentPlugin;
		}

		@Override
		public WritableDataAdapter<SimpleFeature>[] getDataAdapters(
				final String globalVisibility ) {
			return parentPlugin.getDataAdapters(globalVisibility);
		}

		@Override
		public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
				final TdrivePoint input,
				final ByteArrayId primaryIndexId,
				final String globalVisibility ) {
			return parentPlugin.toGeoWaveDataInternal(
					input,
					primaryIndexId,
					globalVisibility);
		}

		@Override
		public byte[] toBinary() {
			return new byte[] {};
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}
	}
}
