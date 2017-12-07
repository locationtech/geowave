package mil.nga.giat.geowave.analytic.spark.sparksql;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomWriter;

@SuppressFBWarnings
public class SimpleFeatureMapper implements
		Function<SimpleFeature, Row>
{
	private static Logger LOGGER = LoggerFactory.getLogger(SimpleFeatureDataFrame.class);

	private final StructType schema;
	private final GeomWriter geomWriter = new GeomWriter();

	public SimpleFeatureMapper(
			StructType schema ) {
		this.schema = schema;
	}

	@Override
	public Row call(
			SimpleFeature feature )
			throws Exception {
		Object[] fields = new Serializable[schema.size()];

		for (int i = 0; i < schema.size(); i++) {
			Object fieldObj = feature.getAttribute(i);
			if (fieldObj != null) {
				StructField structField = schema.apply(i);
				if (structField.name().equals(
						"geom")) {
					fields[i] = geomWriter.write((Geometry) fieldObj);
				}
				else if (structField.dataType() == DataTypes.TimestampType) {
					fields[i] = ((Timestamp) new Timestamp(
							((Date) fieldObj).getTime()));
				}
				else if (structField.dataType() != null) {
					fields[i] = (Serializable) fieldObj;
				}
				else {
					LOGGER.error("Unexpected attribute in field(" + structField.name() + "): " + fieldObj);
				}
			}
		}

		return RowFactory.create(fields);
	}

}
