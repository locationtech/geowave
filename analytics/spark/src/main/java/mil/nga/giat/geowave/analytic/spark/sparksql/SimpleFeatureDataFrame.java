package mil.nga.giat.geowave.analytic.spark.sparksql;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunctionRegistry;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.SchemaConverter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;

public class SimpleFeatureDataFrame
{
	private static Logger LOGGER = LoggerFactory.getLogger(SimpleFeatureDataFrame.class);

	private final SparkSession sparkSession;
	private SimpleFeatureType featureType;
	private StructType schema;
	private JavaRDD<Row> rowRDD = null;
	private Dataset<Row> dataFrame = null;

	public SimpleFeatureDataFrame(
			final SparkSession sparkSession ) {
		this.sparkSession = sparkSession;
	}

	public boolean init(
			final DataStorePluginOptions dataStore,
			final ByteArrayId adapterId ) {
		featureType = FeatureDataUtils.getFeatureType(
				dataStore,
				adapterId);
		if (featureType == null) {
			return false;
		}

		schema = SchemaConverter.schemaFromFeatureType(featureType);
		if (schema == null) {
			return false;
		}

		GeomFunctionRegistry.registerGeometryFunctions(sparkSession);

		return true;
	}

	public SimpleFeatureType getFeatureType() {
		return featureType;
	}

	public StructType getSchema() {
		return schema;
	}

	public JavaRDD<Row> getRowRDD() {
		return rowRDD;
	}

	public Dataset<Row> getDataFrame(
			GeoWaveRDD pairRDD ) {
		if (rowRDD == null) {
			SimpleFeatureMapper mapper = new SimpleFeatureMapper(
					schema);

			rowRDD = pairRDD.getRawRDD().values().map(
					mapper);
		}

		if (dataFrame == null) {
			dataFrame = sparkSession.createDataFrame(
					rowRDD,
					schema);
		}

		return dataFrame;
	}

	public Dataset<Row> resetDataFrame(
			GeoWaveRDD pairRDD ) {
		rowRDD = null;
		dataFrame = null;

		return getDataFrame(pairRDD);
	}
}
