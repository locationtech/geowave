package mil.nga.giat.geowave.format.gdelt;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.ZipInputStream;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.adapter.vector.utils.GeometryUtils;
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

/*
 */
public class GDELTIngestPlugin extends
		AbstractSimpleFeatureIngestPlugin<WholeFile>
{

	private final static Logger LOGGER = Logger.getLogger(
			GDELTIngestPlugin.class);

	private final SimpleFeatureBuilder gdeltEventBuilder;
	private final SimpleFeatureType gdeltEventType;

	private final ByteArrayId eventKey;

	private CoordinateReferenceSystem crs;
	private final static String CRS_AUTHORITY = "EPSG:4326";

	public GDELTIngestPlugin() {
		gdeltEventType = GDELTUtils.createGDELTEventDataType();
		eventKey = new ByteArrayId(
				StringUtils.stringToBinary(
						GDELTUtils.GDELT_EVENT_FEATURE));
		gdeltEventBuilder = new SimpleFeatureBuilder(
				gdeltEventType);

		try {
			crs = CRS.decode(
					CRS_AUTHORITY);
		}
		catch (final FactoryException e) {
			LOGGER.error(
					"Unable to decode Coordinate Reference System authority code!",
					e);
		}
	}

	@Override
	protected SimpleFeatureType[] getTypes() {
		return new SimpleFeatureType[] {
			SimpleFeatureUserDataConfigurationSet.configureType(
					gdeltEventType)
		};
	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"zip"
		};
	}

	@Override
	public void init(
			final File baseDirectory ) {

	}

	@Override
	public boolean supportsFile(
			final File file ) {
		return GDELTUtils.validate(
				file);
	}

	@Override
	public Schema getAvroSchema() {
		return WholeFile.getClassSchema();
	}

	@Override
	public WholeFile[] toAvroObjects(
			final File input ) {
		final WholeFile avroFile = new WholeFile();
		avroFile.setOriginalFilePath(
				input.getAbsolutePath());
		try {
			avroFile.setOriginalFile(
					ByteBuffer.wrap(
							Files.readAllBytes(
									input.toPath())));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read GDELT file: " + input.getAbsolutePath(),
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
		return new IngestGDELTFromHdfs(
				this);
	}

	@Override
	public IngestWithReducer<WholeFile, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GDELT events cannot be ingested with a reducer");
	}

	@Override
	protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final WholeFile hfile,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {

		final List<GeoWaveData<SimpleFeature>> featureData = new ArrayList<GeoWaveData<SimpleFeature>>();

		final InputStream in = new ZipInputStream(
				new ByteArrayInputStream(
						hfile.getOriginalFile().array()));
		final InputStreamReader isr = new InputStreamReader(
				in,
				StringUtils.UTF8_CHAR_SET);
		final BufferedReader br = new BufferedReader(
				isr);

		final GeometryFactory geometryFactory = new GeometryFactory();

		Date timeStamp = null;
		String timestring = "";
		String eventId = "";
		double lat;
		double lon;

		String line;
		try {
			while ((line = br.readLine()) != null) {

				final String[] vals = line.split(
						"\t");
				if ((vals.length < GDELTUtils.GDELT_MIN_COLUMNS) || (vals.length > GDELTUtils.GDELT_MAX_COLUMNS)) {
					LOGGER.warn(
							"Invalid GDELT line length: " + vals.length + " tokens found.");
					continue;
				}

				eventId = vals[GDELTUtils.GDELT_EVENT_ID_COLUMN_ID];

				lat = GeometryUtils.adjustCoordinateDimensionToRange(
						Double.parseDouble(
								vals[GDELTUtils.GDELT_LATITUDE_COLUMN_ID]),
						crs,
						1);
				lon = GeometryUtils.adjustCoordinateDimensionToRange(
						Double.parseDouble(
								vals[GDELTUtils.GDELT_LONGITUDE_COLUMN_ID]),
						crs,
						0);
				final Coordinate cord = new Coordinate(
						lat,
						lon);

				gdeltEventBuilder.set(
						GDELTUtils.GDELT_GEOMETRY_ATTRIBUTE,
						geometryFactory.createPoint(
								cord));

				gdeltEventBuilder.set(
						GDELTUtils.GDELT_EVENT_ID_ATTRIBUTE,
						eventId);

				timestring = vals[GDELTUtils.GDELT_TIMESTAMP_COLUMN_ID];
				timeStamp = GDELTUtils.parseDate(
						timestring);
				gdeltEventBuilder.set(
						GDELTUtils.GDELT_TIMESTAMP_ATTRIBUTE,
						timeStamp);

				gdeltEventBuilder.set(
						GDELTUtils.GDELT_LATITUDE_ATTRIBUTE,
						lat);
				gdeltEventBuilder.set(
						GDELTUtils.GDELT_LONGITUDE_ATTRIBUTE,
						lon);

				featureData.add(
						new GeoWaveData<SimpleFeature>(
								eventKey,
								primaryIndexId,
								gdeltEventBuilder.buildFeature(
										eventId)));
			}

		}
		catch (final IOException e) {
			LOGGER.warn(
					"Error reading line from file: " + hfile.getOriginalFilePath(),
					e);
		}
		catch (final ParseException e) {
			LOGGER.error(
					"Error parsing time string: " + timestring,
					e);
		}
		finally {
			IOUtils.closeQuietly(
					br);
			IOUtils.closeQuietly(
					isr);
			IOUtils.closeQuietly(
					in);
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
		return new IngestGDELTFromHdfs(
				this);
	}

	public static class IngestGDELTFromHdfs extends
			AbstractIngestSimpleFeatureWithMapper<WholeFile>
	{
		public IngestGDELTFromHdfs() {
			this(
					new GDELTIngestPlugin());
		}

		public IngestGDELTFromHdfs(
				final GDELTIngestPlugin parentPlugin ) {
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
