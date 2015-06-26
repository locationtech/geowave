package mil.nga.giat.geowave.format.tdrive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.mortbay.log.Log;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;

/*
 */
public class TdriveIngestPlugin extends
		AbstractSimpleFeatureIngestPlugin<TdrivePoint>
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
			IndexType.SPATIAL_VECTOR.createDefaultIndex(),
			IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex()
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
					SimpleFeatureUserDataConfigurationSet.configureType(tdrivepointType),
					fieldVisiblityHandler)
		};
	}

	@Override
	public Schema getAvroSchema() {
		return TdrivePoint.getClassSchema();
	}

	@Override
	public TdrivePoint[] toAvroObjects(
			final File input ) {
		BufferedReader fr = null;
		BufferedReader br = null;
		long pointInstance = 0l;
		final List<TdrivePoint> pts = new ArrayList<TdrivePoint>();
		try {
			fr = new BufferedReader(
					new InputStreamReader(
							new FileInputStream(
									input),
							StringUtils.UTF8_CHAR_SET));
			br = new BufferedReader(
					fr);
			String line;
			try {
				while ((line = br.readLine()) != null) {

					final String[] vals = line.split(",");
					final TdrivePoint td = new TdrivePoint();
					td.setTaxiid(Integer.parseInt(vals[0]));
					try {
						td.setTimestamp(TdriveUtils.parseDate(
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
		return new IngestTdrivePointFromHdfs(
				this);
	}

	@Override
	public IngestWithReducer<TdrivePoint, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GPX tracks cannot be ingested with a reducer");
	}

	@Override
	protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
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
				"Latitude",
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

	public static class IngestTdrivePointFromHdfs extends
			AbstractIngestSimpleFeatureWithMapper<TdrivePoint>
	{
		public IngestTdrivePointFromHdfs() {
			this(
					new TdriveIngestPlugin());
			// this constructor will be used when deserialized
		}

		public IngestTdrivePointFromHdfs(
				final TdriveIngestPlugin parentPlugin ) {
			super(
					parentPlugin);
		}
	}

	@Override
	public IngestPluginBase<TdrivePoint, SimpleFeature> getIngestWithAvroPlugin() {
		return new IngestTdrivePointFromHdfs(
				this);
	}
}
