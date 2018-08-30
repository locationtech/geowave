package mil.nga.giat.geowave.analytic.spark.sparksql.udt;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;

/**
 * Created by jwileczek on 7/20/18.
 */
public abstract class AbstractGeometryUDT<T extends Geometry> extends
		UserDefinedType<T>
{
	@Override
	public DataType sqlType() {
		return new StructType(
				new StructField[] {
					new StructField(
							"wkb",
							DataTypes.BinaryType,
							true,
							Metadata.empty())
				});
	}

	@Override
	public String pyUDT() {
		return "geowave_pyspark.types." + this.getClass().getSimpleName();
	}

	@Override
	public InternalRow serialize(
			T obj ) {
		byte[] bytes = new WKBWriter().write(obj);
		InternalRow returnRow = new GenericInternalRow(
				bytes.length);
		returnRow.update(
				0,
				bytes);
		return returnRow;
	}

	@Override
	public T deserialize(
			Object datum ) {
		T geom = null;
		InternalRow row = (InternalRow) datum;
		byte[] bytes = row.getBinary(0);
		try {
			geom = (T) new WKBReader().read(bytes);
		}
		catch (ParseException e) {
			e.printStackTrace();
		}
		return geom;
	}

}
