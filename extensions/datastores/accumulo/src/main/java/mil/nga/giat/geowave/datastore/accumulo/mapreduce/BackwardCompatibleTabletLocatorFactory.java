package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TabletId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*if[accumulo.api=1.7]
 import org.apache.accumulo.core.util.UtilWaitThread;
 import org.apache.accumulo.core.client.mock.MockInstance;
 import org.apache.accumulo.core.data.impl.KeyExtent;
 import org.apache.accumulo.core.data.impl.TabletIdImpl;
 import org.apache.accumulo.core.master.state.tables.TableState;
 import org.apache.accumulo.core.client.TableOfflineException;
 import org.apache.accumulo.core.client.impl.ClientContext;
 import org.apache.accumulo.core.client.impl.Credentials;
 import org.apache.accumulo.core.client.impl.Tables;
 import org.apache.accumulo.core.client.impl.TabletLocator;
 import org.apache.accumulo.core.client.security.tokens.NullToken;
 import org.apache.accumulo.core.client.security.tokens.PasswordToken;
 import org.apache.accumulo.core.client.ClientConfiguration;
 import org.apache.accumulo.core.client.Instance;
 import org.apache.accumulo.core.client.TableDeletedException;
 import java.util.ArrayList;
 import java.util.HashMap;
 import java.util.Map.Entry;
 import org.apache.hadoop.io.Text;
 else[accumulo.api=1.7]*/
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.Connector;
/* end[accumulo.api=1.7] */

import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;

class BackwardCompatibleTabletLocatorFactory
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BackwardCompatibleTabletLocatorFactory.class);
	protected static interface BackwardCompatibleTabletLocator
	{
		public Map<TabletId, List<Range>> getLocationsGroupedByTablet();

		public String getTabletLocation(
				TabletId tabletId );

		public Range toRange(
				TabletId tabletId );
	}

	public static BackwardCompatibleTabletLocator createTabletLocator(
			final AccumuloOperations operations,
			final String tableName,
			final TreeSet<Range> ranges )
			throws IOException {
		// @formatter:off
		/*if[accumulo.api=1.7]
		return new Accumulo_1_7_Locator(
				getBinnedRangesStructure(
						operations,
						tableName,
						ranges));
						
		else[accumulo.api=1.7]*/
		// @formatter:on
		try {
			return new Accumulo_1_8_Locator(
					getLocations(
							operations,
							tableName,
							ranges));
		}
		catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
			throw new IOException(
					"Unable to get Tablet Locations",
					e);
		}
		/* end[accumulo.api=1.7] */
	}

	// @formatter:off
	/*if[accumulo.api=1.7]
	protected static class Accumulo_1_7_Locator implements
			BackwardCompatibleTabletLocator
	{
		Map<TabletId, List<Range>> binnedRanges;

		Map<TabletId, String> tabletToLocation;

		public Accumulo_1_7_Locator(
				final Map<String, Map<KeyExtent, List<Range>>> tserverBinnedRanges ) {
			super();
			binnedRanges = new HashMap<>();
			tabletToLocation = new HashMap<>();
			for (final Entry<String, Map<KeyExtent, List<Range>>> tserverBin : tserverBinnedRanges.entrySet()) {
				for (final Entry<KeyExtent, List<Range>> extentRanges : tserverBin.getValue().entrySet()) {
					final TabletIdImpl id = new TabletIdImpl(
							extentRanges.getKey());
					binnedRanges.put(
							id,
							extentRanges.getValue());
					tabletToLocation.put(
							id,
							tserverBin.getKey());
				}
			}
		}

		@Override
		public Map<TabletId, List<Range>> getLocationsGroupedByTablet() {
			return binnedRanges;
		}

		@Override
		public String getTabletLocation(
				final TabletId tabletId ) {
			return tabletToLocation.get(
					tabletId);
		}

		@Override
		public Range toRange(
				final TabletId tabletId ) {
			return new Range(
					tabletId.getPrevEndRow(),
					false,
					tabletId.getEndRow(),
					true);
		}
	}

	//Returns binnedRanges. Extracted out as its own method to facilitate
	//testing
	 
	private static Map<String, Map<KeyExtent, List<Range>>> getBinnedRangesStructure(
			final AccumuloOperations accumuloOperations,
			final String tableName,
			final TreeSet<Range> ranges )
			throws IOException {
		final Map<String, Map<KeyExtent, List<Range>>> tserverBinnedRanges = new HashMap<String, Map<KeyExtent, List<Range>>>();
		TabletLocator tl;
		try {
			final Instance instance = accumuloOperations.getInstance();
			final String tableId;
			Credentials credentials;
			if (instance instanceof MockInstance) {
				tableId = "";
				// in this case, we will have no password;
				credentials = new Credentials(
						accumuloOperations.getUsername(),
						new NullToken());
			}
			else {
				tableId = Tables.getTableId(
						instance,
						tableName);
				credentials = new Credentials(
						accumuloOperations.getUsername(),
						new PasswordToken(
								accumuloOperations.getPassword()));
			}

			final ClientContext clientContext = new ClientContext(
					instance,
					credentials,
					new ClientConfiguration());
			tl = getTabletLocator(
					clientContext,
					tableId);

			final Object clientContextOrCredentials = clientContext;
			// its possible that the cache could contain complete, but
			// old information about a tables tablets... so clear it
			tl.invalidateCache();
			final List<Range> rangeList = new ArrayList<Range>(
					ranges);
			while (!binRanges(
					rangeList,
					clientContextOrCredentials,
					tserverBinnedRanges,
					tl)) {
				if (!(instance instanceof MockInstance)) {
					if (!Tables.exists(
							instance,
							tableId)) {
						throw new TableDeletedException(
								tableId);
					}
					if (Tables.getTableState(
							instance,
							tableId) == TableState.OFFLINE) {
						throw new TableOfflineException(
								instance,
								tableId);
					}
				}
				tserverBinnedRanges.clear();
				LOGGER.warn(
						"Unable to locate bins for specified ranges. Retrying.");
				UtilWaitThread.sleep(
						150);
				tl.invalidateCache();
			}
		}
		catch (final Exception e) {
			throw new IOException(
					e);
		}
		return tserverBinnedRanges;
	}

	private static TabletLocator getTabletLocator(
			final Object clientContextOrInstance,
			final String tableId )
			throws TableNotFoundException {
		TabletLocator tabletLocator = null;

		tabletLocator = TabletLocator.getLocator(
				(ClientContext) clientContextOrInstance,
				new Text(
						tableId));
		return tabletLocator;
	}

	private static boolean binRanges(
			final List<Range> rangeList,
			final Object clientContextOrCredentials,
			final Map<String, Map<KeyExtent, List<Range>>> tserverBinnedRanges,
			final TabletLocator tabletLocator )
			throws AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException,
			IOException {
		return tabletLocator.binRanges(
				(ClientContext) clientContextOrCredentials,
				rangeList,
				tserverBinnedRanges).isEmpty();
	}
	else[accumulo.api=1.7]*/
	// @formatter:on
	protected static class Accumulo_1_8_Locator implements
			BackwardCompatibleTabletLocator
	{
		private Locations locations;

		public Accumulo_1_8_Locator(
				Locations locations ) {
			this.locations = locations;
		}

		@Override
		public Map<TabletId, List<Range>> getLocationsGroupedByTablet() {
			return locations.groupByTablet();
		}

		@Override
		public String getTabletLocation(
				TabletId tabletId ) {
			return locations.getTabletLocation(tabletId);
		}

		@Override
		public Range toRange(
				TabletId tabletId ) {
			return tabletId.toRange();
		}
	}

	private static Locations getLocations(
			final AccumuloOperations operations,
			final String tableName,
			final TreeSet<Range> ranges )
			throws AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException {
		final Connector conn = operations.getConnector();
		return conn.tableOperations().locate(
				tableName,
				ranges);
	}
	/* end[accumulo.api=1.7] */
}
