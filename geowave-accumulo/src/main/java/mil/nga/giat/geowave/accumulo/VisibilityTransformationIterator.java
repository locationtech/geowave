package mil.nga.giat.geowave.accumulo;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.index.ByteArrayUtils;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.hadoop.io.Text;

public class VisibilityTransformationIterator extends TransformingIterator {

	private String transformingRegex;
	private String replacement;
	protected static final String REGEX_ID = "transformingRegex";
	protected static final String REPLACEMENT_ID = "replacement";

	public VisibilityTransformationIterator() {
		super();
	}


	public VisibilityTransformationIterator(String transformingRegex,
			String replacement) {
		super();
		this.transformingRegex = transformingRegex;
		this.replacement = replacement;
	}


	public static void populate(IteratorSetting iteratorSettings, String[] additionalAuthorizations,
			String transformingRegex, String replacement) {
		iteratorSettings.addOption(
				REGEX_ID,
				ByteArrayUtils.byteArrayToString(transformingRegex.getBytes()));
		iteratorSettings.addOption(
				REPLACEMENT_ID,
				ByteArrayUtils.byteArrayToString(replacement.getBytes()));
		if (additionalAuthorizations.length >0)
		   iteratorSettings.addOption(AUTH_OPT, authString(additionalAuthorizations));
	}
	
	private static String authString(String[] authorizations) {
		StringBuffer buffer = new StringBuffer();
		for (String authorization: authorizations) {
			if (buffer.length() > 0) buffer.append(',');
			buffer.append(authorization);
		}
		return buffer.toString();
	}
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		super.init(source, options, env);
		transformingRegex = new String( ByteArrayUtils.byteArrayFromString(options.get(REGEX_ID)));
		replacement = new String(ByteArrayUtils.byteArrayFromString( options.get(REPLACEMENT_ID)));
	}


	@Override
	protected PartialKey getKeyPrefix() {
		return PartialKey.ROW_COLFAM_COLQUAL;
	}

	@Override
	protected void transformRange(SortedKeyValueIterator<Key, Value> input,
			KVBuffer output) throws IOException {
		while (input.hasTop()) {
			Key originalKey = input.getTopKey();
			Value value = input.getTopValue();
			Text visibiltity = originalKey.getColumnVisibility();
			String newVisibility = visibiltity.toString().replaceFirst(
					transformingRegex, replacement);
			if (newVisibility.length() > 0) {
			  char one = newVisibility.charAt(0);
			  // strip off any ending options
			  if (one == '&' || one == '|')
				newVisibility = newVisibility.substring(1);
			}
			byte[] row = originalKey.getRowData().toArray();
			byte[] cf = originalKey.getColumnFamilyData().toArray();
			byte[] cq = originalKey.getColumnQualifierData().toArray();
			long timestamp = originalKey.getTimestamp();
			byte[] cv = newVisibility.getBytes();
			Key newKey = new Key(row, 0, row.length, cf, 0, cf.length, cq, 0,
					cq.length, cv, 0, cv.length, timestamp);
			newKey.setDeleted(originalKey.isDeleted());
			output.append(newKey, value);
			input.next();
		}
	}

}
