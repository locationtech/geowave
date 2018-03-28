package mil.nga.giat.geowave.core.store.filter;

import java.util.Set;

import com.google.common.base.Predicate;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.util.VisibilityExpression;

/**
 * Provides a visibility filter for UNMERGED rows. The filter only operates on
 * the first {@link GeoWaveValue} of each row and must be applied prior to row
 * merging.
 */
public class ClientVisibilityFilter implements
		Predicate<GeoWaveRow>
{
	private final Set<String> auths;

	public ClientVisibilityFilter(
			Set<String> auths ) {
		this.auths = auths;
	}

	@Override
	public boolean apply(
			GeoWaveRow input ) {
		String visibility = "";
		GeoWaveValue[] fieldValues = input.getFieldValues();
		if (fieldValues.length > 0 && fieldValues[0].getVisibility() != null) {
			visibility = StringUtils.stringFromBinary(input.getFieldValues()[0].getVisibility());
		}
		return VisibilityExpression.evaluate(
				visibility,
				auths);
	}

}
