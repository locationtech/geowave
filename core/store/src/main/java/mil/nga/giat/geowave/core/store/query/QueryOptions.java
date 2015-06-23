package mil.nga.giat.geowave.core.store.query;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.StringUtils;

/**
 * Container object that encapsulates additional options to be applied to a
 * {@link Query}
 * 
 * @since 0.8.7
 */
public class QueryOptions implements
		Persistable,
		Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 544085046847603372L;
	private Collection<String> fieldIds = Collections.emptyList();

	/**
	 * @param fieldIds
	 *            the desired subset of fieldIds to be included in query results
	 */
	public QueryOptions(
			final Collection<String> fieldIds ) {
		super();
		this.fieldIds = fieldIds;
	}

	public QueryOptions() {

	}

	/**
	 * @param fieldIds
	 *            comma-separated list of field IDS
	 */
	public QueryOptions(
			final String fieldIds ) {
		super();
		final String[] ids = fieldIds.split(",");
		this.fieldIds = Arrays.asList(ids);
	}

	/**
	 * @return the fieldIds or an empty List, will never return null
	 */
	public Collection<String> getFieldIds() {
		if (fieldIds == null) {
			fieldIds = Collections.emptyList();
		}
		return fieldIds;
	}

	/**
	 * @param fieldIds
	 *            the desired subset of fieldIds to be included in query results
	 */
	public void setFieldIds(
			final Collection<String> fieldIds ) {
		this.fieldIds = fieldIds;
	}

	@Override
	public byte[] toBinary() {
		if (fieldIds == null) return new byte[0];
		final StringBuffer buffer = new StringBuffer();
		for (final String fieldId : fieldIds) {
			if (buffer.length() > 0) {
				buffer.append(",");
			}
			buffer.append(fieldId);
		}
		return StringUtils.stringToBinary(buffer.toString());
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final String data = StringUtils.stringFromBinary(bytes);
		final String[] ids = data.split(",");
		fieldIds = Arrays.asList(ids);
	}

}
