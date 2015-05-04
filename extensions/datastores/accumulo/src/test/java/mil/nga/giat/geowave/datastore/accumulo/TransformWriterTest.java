package mil.nga.giat.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.Writer;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.TransformerWriter;
import mil.nga.giat.geowave.datastore.accumulo.util.VisibilityTransformer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class TransformWriterTest
{
	private final MockInstance mockInstance = new MockInstance();
	private Connector mockConnector = null;
	private BasicAccumuloOperations operations;

	@Before
	public void setUp() {

		try {
			mockConnector = mockInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));

			operations = new BasicAccumuloOperations(
					mockConnector);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			e.printStackTrace();
		}

		operations.createTable("test_table");
	}

	private void write(
			Writer writer,
			String id,
			String cf,
			String cq,
			String vis,
			String value ) {
		Mutation m = new Mutation(
				new Text(
						id.getBytes(StringUtils.UTF8_CHAR_SET)));
		m.put(
				new Text(
						cf),
				new Text(
						cq),
				new ColumnVisibility(
						vis),
				new Value(
						value.getBytes(StringUtils.UTF8_CHAR_SET)));
		writer.write(m);
	}

	private class Expect
	{
		byte[] id;
		int count;

		public Expect(
				byte[] id,
				int count ) {
			super();
			this.id = id;
			this.count = count;
		}

	}

	private void check(
			Iterator<Entry<Key, Value>> it,
			Expect... expectations ) {
		Map<ByteArrayId, Integer> result = new HashMap<ByteArrayId, Integer>();

		while (it.hasNext()) {
			Entry<Key, Value> entry = it.next();
			ByteArrayId rowID = new ByteArrayId(
					entry.getKey().getRow().getBytes());
			result.put(
					rowID,
					Integer.valueOf(1 + (result.containsKey(rowID) ? result.get(
							rowID).intValue() : 0)));
		}
		int expectedCount = 0;
		for (Expect e : expectations) {
			ByteArrayId rowID = new ByteArrayId(
					e.id);
			expectedCount += (e.count > 0 ? 1 : 0);
			assertEquals(
					new Text(
							e.id).toString(),
					e.count,
					(result.containsKey(rowID) ? result.get(
							rowID).intValue() : 0));
		}
		assertEquals(
				result.size(),
				expectedCount);
	}

	@Test
	public void test()
			throws TableNotFoundException,
			MutationsRejectedException {
		Writer w = operations.createWriter("test_table");
		write(
				w,
				"1234",
				"cf1",
				"cq1",
				"a&b",
				"123");
		write(
				w,
				"1234",
				"cf2",
				"cq2",
				"a&b",
				"123");
		write(
				w,
				"1235",
				"cf1",
				"cq1",
				"a&b",
				"123");
		write(
				w,
				"1235",
				"cf2",
				"cq2",
				"a&b",
				"123");
		w.close();

		Scanner scanner = operations.createScanner(
				"test_table",
				"a",
				"b");
		check(
				scanner.iterator(),
				new Expect(
						"1234".getBytes(StringUtils.UTF8_CHAR_SET),
						2),
				new Expect(
						"1235".getBytes(StringUtils.UTF8_CHAR_SET),
						2));
		scanner.close();

		scanner = operations.createScanner(
				"test_table",
				"a",
				"c");
		check(
				scanner.iterator(),
				new Expect(
						"1234".getBytes(StringUtils.UTF8_CHAR_SET),
						0),
				new Expect(
						"1235".getBytes(StringUtils.UTF8_CHAR_SET),
						0));
		scanner.close();

		VisibilityTransformer transformer = new VisibilityTransformer(
				"b",
				"c");
		scanner = operations.createScanner(
				"test_table",
				"a",
				"b",
				"c");
		TransformerWriter tw = new TransformerWriter(
				scanner,
				"test_table",
				operations,
				transformer);
		tw.transform();
		scanner.close();

		scanner = operations.createScanner(
				"test_table",
				"a",
				"c");
		check(
				scanner.iterator(),
				new Expect(
						"1234".getBytes(),
						2),
				new Expect(
						"1235".getBytes(),
						2));
		scanner.close();

		scanner = operations.createScanner(
				"test_table",
				"a",
				"b");
		check(
				scanner.iterator(),
				new Expect(
						"1234".getBytes(),
						0),
				new Expect(
						"1235".getBytes(),
						0));
		scanner.close();

	}
}
