package mil.nga.giat.geowave.types.geolife;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.ingest.hdfs.HdfsFile;
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
import mil.nga.giat.geowave.types.tdrive.TdrivePoint;
import mil.nga.giat.geowave.types.tdrive.TdriveUtils;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.IOUtil;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.mortbay.log.Log;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.xml.sax.SAXException;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Coordinate;

/*
 */
public class GeoLifeIngestPlugin implements
		LocalFileIngestPlugin<SimpleFeature>,
		IngestFromHdfsPlugin<HdfsFile, SimpleFeature>,
		StageToHdfsPlugin<HdfsFile>
{

	private final static Logger LOGGER = Logger.getLogger(GeoLifeIngestPlugin.class);

	
	private final SimpleFeatureBuilder geolifePointBuilder;
	private final SimpleFeatureType geolifePointType;
	
	private final SimpleFeatureBuilder geolifeTrackBuilder;
	private final SimpleFeatureType geolifeTrackType;
	
	private final ByteArrayId pointKey;
	private final ByteArrayId trackKey;

	private final Index[] supportedIndices;

	public GeoLifeIngestPlugin() {

		geolifePointType = GeoLifeUtils.createGeoLifePointDataType();
		pointKey = new ByteArrayId(
				StringUtils.stringToBinary(GeoLifeUtils.GEOLIFE_POINT_FEATURE));
		geolifePointBuilder = new SimpleFeatureBuilder(geolifePointType);
		
		
		geolifeTrackType = GeoLifeUtils.createGeoLifeTrackDataType();
		trackKey = new ByteArrayId(
				StringUtils.stringToBinary(GeoLifeUtils.GEOLIFE_TRACK_FEATURE));
		geolifeTrackBuilder = new SimpleFeatureBuilder(geolifeTrackType);

		
		supportedIndices = new Index[] {
			IndexType.SPATIAL.createDefaultIndex(),
			IndexType.SPATIAL_TEMPORAL.createDefaultIndex()
		};

	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"plt"
		};
	}

	@Override
	public void init(final File baseDirectory ) {
		
	}

	@Override
	public boolean supportsFile(final File file ) {
			return GeoLifeUtils.validate(file);
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
					geolifePointType,
					fieldVisiblityHandler),
			new FeatureDataAdapter(
					geolifeTrackType,
					fieldVisiblityHandler)
					
		};
	}

	@Override
	public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
			final File input,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {
		final HdfsFile[] geolifeFiles = toHdfsObjects(input);
		final List<CloseableIterator<GeoWaveData<SimpleFeature>>> allData = new ArrayList<CloseableIterator<GeoWaveData<SimpleFeature>>>();
		for (final HdfsFile file : geolifeFiles) {
			final CloseableIterator<GeoWaveData<SimpleFeature>> geowaveData = toGeoWaveDataInternal(
					file,
					primaryIndexId,
					globalVisibility);
			allData.add(geowaveData);
		}
		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(
				Iterators.concat(allData.iterator()));
	}

	@Override
	public Schema getAvroSchemaForHdfsType() {
		return HdfsFile.getClassSchema();
	}

	@Override
	public HdfsFile[] toHdfsObjects(final File input ) {
		HdfsFile hfile = new HdfsFile();
		hfile.setOriginalFilePath(input.getAbsolutePath());
		try {
			hfile.setOriginalFile(ByteBuffer.wrap(Files.readAllBytes(input.toPath())));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read GeoLife file: " + input.getAbsolutePath(),
					e);
			return new HdfsFile[] {};
		}
		
		return new HdfsFile[] {hfile};
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<HdfsFile, SimpleFeature> ingestWithMapper() {
		return new IngestGpxTrackFromHdfs(this);
	}

	@Override
	public IngestWithReducer<HdfsFile, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoLife tracks cannot be ingested with a reducer");
	}

	private CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final HdfsFile hfile,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {

		
		final List<GeoWaveData<SimpleFeature>> featureData = new ArrayList<GeoWaveData<SimpleFeature>>();
		
		
        InputStream in = new ByteArrayInputStream(hfile.getOriginalFile().array());
        InputStreamReader isr = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(isr);
    	int pointInstance = 0;
		List<Coordinate> pts = new ArrayList<Coordinate>();
		String trackId = FilenameUtils.getName(hfile.getOriginalFilePath().toString());
		String line;
		Date startTimeStamp = null;
		Date endTimeStamp = null;
		String timestring = "";
		try {
				while ((line = br.readLine()) != null){
					
					String[] vals = line.split(",");
					if (vals.length != 7) 
						continue;
				
					Coordinate cord = new Coordinate(Double.parseDouble(vals[1]), Double.parseDouble(vals[0]));
					pts.add(cord);
					geolifePointBuilder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(cord));
					geolifePointBuilder.set("trackid", trackId);
					geolifePointBuilder.set("pointinstance", pointInstance);
					pointInstance++;
					
					timestring = vals[5] + " " + vals[6];
					Date ts = GeoLifeUtils.TIME_FORMAT_SECONDS.parse(timestring);
					geolifePointBuilder.set("Timestamp", ts);
					if (startTimeStamp == null){
						startTimeStamp = ts;
					}
					endTimeStamp = ts;
					
										
					geolifePointBuilder.set("Latitude", Double.parseDouble(vals[0]));
					geolifePointBuilder.set("Longitude", Double.parseDouble(vals[1]));
					
					Double elevation = Double.parseDouble(vals[3]);
					if (elevation == -777) //magic number form spec
						elevation = null;
					geolifePointBuilder.set("Elevation", elevation);
					featureData.add(new GeoWaveData<SimpleFeature>(
							pointKey,
							primaryIndexId,
							geolifePointBuilder.buildFeature(trackId + "_" + pointInstance)));
				}
				
				geolifeTrackBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createLineString(pts.toArray(new Coordinate[pts.size()])));
				
				geolifeTrackBuilder.set("StartTimeStamp", startTimeStamp);
				geolifeTrackBuilder.set("EndTimeStamp", endTimeStamp);
				geolifeTrackBuilder.set("Duration",endTimeStamp.getTime() - startTimeStamp.getTime());
				geolifeTrackBuilder.set("NumberPoints", pointInstance);
				geolifeTrackBuilder.set("TrackId", trackId);
				featureData.add(new GeoWaveData<SimpleFeature>(
						trackKey,
						primaryIndexId,
						geolifeTrackBuilder.buildFeature(trackId)));
				
				
			} catch (IOException e) {
				LOGGER.warn("Error reading line from file: " + hfile.getOriginalFilePath(), e);
			} catch (ParseException e) {
				LOGGER.error("Error parsing time string: " + timestring, e);
			}
		finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(isr);
			IOUtils.closeQuietly(in);
		}
		
	
		
		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(featureData.iterator());
	}

	@Override
	public Index[] getRequiredIndices() {
		return new Index[] {};
	}

	public static class IngestGpxTrackFromHdfs implements
			IngestWithMapper<HdfsFile, SimpleFeature>
	{
		private final GeoLifeIngestPlugin parentPlugin;

		public IngestGpxTrackFromHdfs() {
			this(
					new GeoLifeIngestPlugin());
		}

		public IngestGpxTrackFromHdfs(
				final GeoLifeIngestPlugin parentPlugin ) {
			this.parentPlugin = parentPlugin;
		}

		@Override
		public WritableDataAdapter<SimpleFeature>[] getDataAdapters(
				final String globalVisibility ) {
			return parentPlugin.getDataAdapters(globalVisibility);
		}

		@Override
		public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
				final HdfsFile input,
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
