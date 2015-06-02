package mil.nga.giat.geowave.examples.ingest.bulk;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCellGenerator;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;

public class SimpleFeatureToHBaseKeyValueMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, ImmutableBytesWritable>
{

	private WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
			GeonamesSimpleFeatureType.getInstance());
	private final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
	private final VisibilityWriter<SimpleFeature> visibilityWriter = new UniformVisibilityWriter<SimpleFeature>(
			new UnconstrainedVisibilityHandler<SimpleFeature, Object>());
	private HBaseCellGenerator<SimpleFeature> generator = new HBaseCellGenerator<SimpleFeature>(
			adapter,
			index,
			visibilityWriter);
	private SimpleFeature simpleFeature;
	private List<Cell> keyValuePairs;
	private final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
			GeonamesSimpleFeatureType.getInstance());
	private String[] geonamesEntryTokens;
	private String geonameId;
	private double longitude;
	private double latitude;
	private String location;

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context )
			throws IOException,
			InterruptedException {

		simpleFeature = parseGeonamesValue(value);

		// build Geowave-formatted Accumulo [Key,Value] pairs
		keyValuePairs = generator.constructKeyValuePairs(
				adapter.getAdapterId().getBytes(),
				simpleFeature);

		// output each [Key,Value] pair to shuffle-and-sort phase where we rely
		// on MapReduce to sort by Key
		for (Cell keyValue : keyValuePairs) {
			context.write(
					new ImmutableBytesWritable(
							CellUtil.cloneRow(keyValue)),
					new ImmutableBytesWritable(
							CellUtil.cloneValue(keyValue)));
		}
	}

	private SimpleFeature parseGeonamesValue(
			Text value ) {

		geonamesEntryTokens = value.toString().split(
				"\\t"); // Exported Geonames entries are tab-delimited

		geonameId = geonamesEntryTokens[0];
		location = geonamesEntryTokens[1];
		latitude = Double.parseDouble(geonamesEntryTokens[4]);
		longitude = Double.parseDouble(geonamesEntryTokens[5]);

		return buildSimpleFeature(
				geonameId,
				longitude,
				latitude,
				location);
	}

	private SimpleFeature buildSimpleFeature(
			String featureId,
			double longitude,
			double latitude,
			String location ) {

		builder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						longitude,
						latitude)));
		builder.set(
				"Latitude",
				latitude);
		builder.set(
				"Longitude",
				longitude);
		builder.set(
				"Location",
				location);

		return builder.buildFeature(featureId);
	}

}
