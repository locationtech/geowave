package mil.nga.giat.geowave.cli.osm.accumulo.osmschema;

/**
 * Created by bennight on 1/31/2015.
 */
public class ColumnFamily
{
	public static final byte[] NODE = "n".getBytes(Constants.CHARSET);
	public static final byte[] WAY = "w".getBytes(Constants.CHARSET);
	public static final byte[] RELATION = "r".getBytes(Constants.CHARSET);
}
