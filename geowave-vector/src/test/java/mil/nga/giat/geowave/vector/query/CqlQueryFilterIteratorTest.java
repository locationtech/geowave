package mil.nga.giat.geowave.vector.query;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.plugin.GeoWaveGTMemDataStore;
import mil.nga.giat.geowave.vector.query.cql.FilterToCQLTool;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.feature.SchemaException;
import org.geotools.filter.FilterFactoryImpl;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.Expression;

import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class CqlQueryFilterIteratorTest
{

	@Test
	public void test()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			IOException,
			ParseException,
			TableNotFoundException {
		final GeoWaveGTMemDataStore dataStore = new GeoWaveGTMemDataStore(
				"CqlQueryFilterIteratorTest");

		final SimpleFeatureType type = DataUtilities.createType(
				"CqlQueryFilterIteratorTest",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String");

		dataStore.createSchema(type);

		final Transaction transaction1 = new DefaultTransaction();

		final FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
				"CqlQueryFilterIteratorTest",
				transaction1);
		final SimpleFeature newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				Long.valueOf(100));
		newFeature.setAttribute(
				"pid",
				"a89dhd-123-dxc");
		newFeature.setAttribute(
				"geometry",
				new WKTReader().read("LINESTRING (30 10, 10 30, 40 40)"));
		writer.write();
		writer.close();

		transaction1.commit();

		final FilterFactoryImpl factory = new FilterFactoryImpl();
		final Expression exp1 = factory.property("pid");
		final Expression exp2 = factory.literal("a89dhd-123-dxc");
		final Filter f = factory.equal(
				exp1,
				exp2,
				false);

		final MockInstance mockDataInstance = new MockInstance(
				"CqlQueryFilterIteratorTest");
		final Connector mockDataConnector = mockDataInstance.getConnector(
				"root",
				new PasswordToken(
						new byte[0]));
		final BasicAccumuloOperations dataOps = new BasicAccumuloOperations(
				mockDataConnector);

		AccumuloIndexStore indexStore = new AccumuloIndexStore(
				dataOps);

		final String tableName = IndexType.SPATIAL_VECTOR.getDefaultId();
		final ScannerBase scanner = dataOps.createScanner(tableName);

		AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				dataOps);

		initScanner(
				scanner,
				indexStore.getIndex(new ByteArrayId(
						IndexType.SPATIAL_VECTOR.getDefaultId())),
				(DataAdapter<SimpleFeature>) adapterStore.getAdapter(new ByteArrayId(
						"CqlQueryFilterIteratorTest")),
				f);

		Iterator<Entry<Key, Value>> it = scanner.iterator();
		assertTrue(it.hasNext());
		int count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		// line string covers more than one tile
		assertTrue(count >= 1);

	}

	private void initScanner(
			ScannerBase scanner,
			Index index,
			DataAdapter<SimpleFeature> dataAdapter,
			Filter cqlFilter ) {
		final IteratorSetting iteratorSettings = new IteratorSetting(
				CqlQueryFilterIterator.CQL_QUERY_ITERATOR_PRIORITY,
				CqlQueryFilterIterator.CQL_QUERY_ITERATOR_NAME,
				CqlQueryFilterIterator.class);
		iteratorSettings.addOption(
				CqlQueryFilterIterator.CQL_FILTER,
				FilterToCQLTool.toCQL(cqlFilter));
		iteratorSettings.addOption(
				CqlQueryFilterIterator.DATA_ADAPTER,
				ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(dataAdapter)));
		iteratorSettings.addOption(
				CqlQueryFilterIterator.MODEL,
				ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index.getIndexModel())));

		scanner.addScanIterator(iteratorSettings);
	}

}
