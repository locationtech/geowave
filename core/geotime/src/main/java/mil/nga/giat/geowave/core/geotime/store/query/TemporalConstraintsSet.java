package mil.nga.giat.geowave.core.geotime.store.query;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Constraints per each property name referenced in a query.
 * 
 */
public class TemporalConstraintsSet
{
	final Map<String, TemporalConstraints> constraintsSet = new HashMap<String, TemporalConstraints>();

	public TemporalConstraintsSet() {}

	public boolean hasConstraintsForRange(
			final String startName,
			final String endName ) {
		return constraintsSet.containsKey(startName + "_" + endName);
	}

	public TemporalConstraints getConstraintsForRange(
			final String startName,
			final String endName ) {
		final String rangeName = startName + "_" + endName;
		if (constraintsSet.containsKey(rangeName)) {
			return constraintsSet.get(rangeName);
		}
		else {
			final TemporalConstraints constraints = new TemporalConstraints(
					rangeName);
			constraintsSet.put(
					rangeName,
					constraints);
			return constraints;
		}
	}

	public TemporalConstraints getConstraintsFor(
			final String fieldName ) {
		if (constraintsSet.containsKey(fieldName)) {
			return constraintsSet.get(fieldName);
		}
		else {
			final TemporalConstraints constraints = new TemporalConstraints(
					fieldName);
			constraintsSet.put(
					fieldName,
					constraints);
			return constraints;
		}
	}

	public void removeConstraints(
			final String... names ) {
		for (String name : names)
			constraintsSet.remove(name);
	}

	public void removeAllConstraintsExcept(
			final String... names ) {
		final Map<String, TemporalConstraints> newConstraintsSet = new HashMap<String, TemporalConstraints>();
		for (final String name : names) {
			final TemporalConstraints constraints = constraintsSet.get(name);
			if (constraints != null) {
				newConstraintsSet.put(
						name,
						constraints);
			}
		}
		constraintsSet.clear();
		constraintsSet.putAll(newConstraintsSet);
	}

	public boolean hasConstraintsFor(
			final String propertyName ) {
		return (propertyName != null) && constraintsSet.containsKey(propertyName);
	}

	public Set<Entry<String, TemporalConstraints>> getSet() {
		return constraintsSet.entrySet();
	}

	public boolean isEmpty() {

		if (constraintsSet.isEmpty()) {
			return true;
		}
		boolean isEmpty = true;
		for (final Entry<String, TemporalConstraints> entry : getSet()) {
			isEmpty &= entry.getValue().isEmpty();
		}
		return isEmpty;
	}
}
