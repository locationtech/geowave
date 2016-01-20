package mil.nga.giat.geowave.cli.debug;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class BBOXQuery extends
		AbstractGeoWaveQuery
{
	private Geometry geom;

	@Override
	protected void applyOptions(
			final Options options ) {

		final Option east = new Option(
				"east",
				true,
				"East in degrees longitude");
		east.setRequired(true);
		final Option west = new Option(
				"west",
				true,
				"West in degrees longitude");
		west.setRequired(true);
		final Option north = new Option(
				"north",
				true,
				"North in degrees latitude");
		north.setRequired(true);
		final Option south = new Option(
				"south",
				true,
				"South in degrees latitude");
		south.setRequired(true);

		options.addOption(west);
		options.addOption(east);
		options.addOption(north);
		options.addOption(south);
	}

	@Override
	protected void parseOptions(
			final CommandLine commandLine ) {
		final double east = Double.parseDouble(commandLine.getOptionValue("east"));
		final double west = Double.parseDouble(commandLine.getOptionValue("west"));
		final double north = Double.parseDouble(commandLine.getOptionValue("north"));
		final double south = Double.parseDouble(commandLine.getOptionValue("south"));
		geom = new GeometryFactory().toGeometry(new Envelope(
				west,
				east,
				south,
				north));
	}

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final DataStore dataStore,
			final boolean debug ) {
		long count = 0;
		try (final CloseableIterator<Object> it = dataStore.query(
				new QueryOptions(
						adapterId,
						null),
				new SpatialQuery(
						geom))) {
			while (it.hasNext()) {
				if (debug) {
					System.out.println(it.next());
				}
				else {
					it.next();
				}
				count++;
			}
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
		return count;
	}

}
