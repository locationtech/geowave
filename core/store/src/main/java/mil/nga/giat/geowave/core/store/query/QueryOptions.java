package mil.nga.giat.geowave.core.store.query;

import java.util.Collection;
import java.util.Collections;

/**
 * Container object that encapsulates additional options to be applied to a
 * {@link Query}
 * 
 * @since 0.8.7
 */
public class QueryOptions
{
	private Collection<String> fieldIds;

	/**
	 * @param fieldIds
	 *            the desired subset of fieldIds to be included in query results
	 */
	public QueryOptions(
			Collection<String> fieldIds ) {
		super();
		this.fieldIds = fieldIds;
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
			Collection<String> fieldIds ) {
		this.fieldIds = fieldIds;
	}

}
