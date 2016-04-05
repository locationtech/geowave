package mil.nga.giat.geowave.core.store.adapter.exceptions;

import java.io.IOException;
import java.util.Arrays;

import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;

/*
 * Thrown if a mapping exists and the new mapping is not equivalent.
 */
public class MismatchedIndexToAdapterMapping extends
		IOException
{

	private static final long serialVersionUID = 1L;

	public MismatchedIndexToAdapterMapping(
			final AdapterToIndexMapping adapterMapping ) {
		super(
				"Adapter " + adapterMapping.getAdapterId() + " already associated to indices " + Arrays.asList(
						adapterMapping.getIndexIds()).toString());
	}

}
