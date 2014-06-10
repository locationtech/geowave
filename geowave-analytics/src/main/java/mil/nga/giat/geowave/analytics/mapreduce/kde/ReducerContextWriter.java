package mil.nga.giat.geowave.analytics.mapreduce.kde;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.accumulo.Writer;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerContextWriter implements
		Writer
{
	private static class KeyValuePair
	{
		private final Key key;
		private final Value value;

		private KeyValuePair(
				final Key key,
				final Value value ) {
			this.key = key;
			this.value = value;
		}

		public Key getKey() {
			return key;
		}

		public Value getValue() {
			return value;
		}
	}

	private final Context context;
	private final String tableName;

	public ReducerContextWriter(
			final Context context,
			final String tableName ) {
		this.context = context;
		this.tableName = tableName;
	}

	@Override
	public void write(
			final Iterable<Mutation> mutations ) {
		for (final Mutation mutation : mutations) {
			write(mutation);
		}
	}

	private List<KeyValuePair> mutationToKeyValuePairs(
			final Mutation m ) {
		final List<KeyValuePair> pairs = new ArrayList<KeyValuePair>();
		final List<ColumnUpdate> updates = m.getUpdates();
		for (final ColumnUpdate cu : updates) {
			pairs.add(new KeyValuePair(
					new Key(
							m.getRow(),
							cu.getColumnFamily(),
							cu.getColumnQualifier(),
							cu.getColumnVisibility(),
							cu.getTimestamp()),
					new Value(
							cu.getValue())));
		}
		return pairs;
	}

	@Override
	public void write(
			final Mutation mutation ) {
		try {
			context.write(
					new Text(
							tableName),
					mutation);
		}
		catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		// final List<KeyValuePair> kvs = mutationToKeyValuePairs(mutation);
		// for (final KeyValuePair kv : kvs) {
		// try {
		// context.write(
		// kv.getKey(),
		// kv.getValue());
		// }
		// catch (IOException | InterruptedException e) {
		// e.printStackTrace();
		// }
		// }

	}

	@Override
	public void close() {}

}
