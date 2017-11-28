package mil.nga.giat.geowave.analytic.spark.sparksql;

import org.apache.spark.sql.types.DataType;

public class SimpleFeatureDataType
{
	private final DataType dataType;
	private final boolean isGeom;

	public SimpleFeatureDataType(
			DataType dataType,
			boolean isGeom ) {
		this.dataType = dataType;
		this.isGeom = isGeom;
	}

	public DataType getDataType() {
		return dataType;
	}

	public boolean isGeom() {
		return isGeom;
	}
}
