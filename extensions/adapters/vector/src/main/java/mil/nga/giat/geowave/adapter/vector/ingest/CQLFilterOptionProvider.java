package mil.nga.giat.geowave.adapter.vector.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterVisitor;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.converters.GeoWaveBaseConverter;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.StringUtils;

/**
 * Supports converting the filter string to Filter object.
 */
public class CQLFilterOptionProvider implements
		Filter,
		Persistable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(CQLFilterOptionProvider.class);

	@Parameter(names = "--cql", description = "A CQL filter, only data matching this filter will be ingested", converter = ConvertCQLStrToFilterConverter.class)
	private FilterParameter convertedFilter = new FilterParameter(
			null,
			null);

	public String getCqlFilterString() {
		return convertedFilter.getCqlFilterString();
	}

	@Override
	public byte[] toBinary() {
		if (convertedFilter.getCqlFilterString() == null) {
			return new byte[] {};
		}
		return StringUtils.stringToBinary(convertedFilter.getCqlFilterString());
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (bytes.length > 0) {
			// This has the side-effect of setting the 'filter' member
			// variable.
			convertedFilter = new ConvertCQLStrToFilterConverter().convert(StringUtils.stringFromBinary(bytes));
		}
		else {
			convertedFilter.setCqlFilterString(null);
			convertedFilter.setFilter(null);
		}
	}

	@Override
	public boolean evaluate(
			final Object object ) {
		if (convertedFilter.getFilter() == null) {
			return true;
		}
		return convertedFilter.getFilter().evaluate(
				object);
	}

	@Override
	public Object accept(
			final FilterVisitor visitor,
			final Object extraData ) {
		if (convertedFilter.getFilter() == null) {
			if (visitor != null) {
				return visitor.visitNullFilter(extraData);
			}
			return extraData;
		}
		return convertedFilter.getFilter().accept(
				visitor,
				extraData);
	}

	private static Filter asFilter(
			final String cqlPredicate )
			throws CQLException {
		return CQL.toFilter(cqlPredicate);
	}

	/**
	 * This class will ensure that as the CQLFilterString is read in and
	 * converted to a filter.
	 */
	public static class ConvertCQLStrToFilterConverter extends
			GeoWaveBaseConverter<FilterParameter>
	{
		public ConvertCQLStrToFilterConverter() {
			super(
					"");
		}

		public ConvertCQLStrToFilterConverter(
				String optionName ) {
			super(
					optionName);
		}

		@Override
		public FilterParameter convert(
				String value ) {
			Filter convertedFilter = null;
			if (value != null) {
				try {
					convertedFilter = asFilter(value);
				}
				catch (final CQLException e) {
					LOGGER.error(
							"Cannot parse CQL expression '" + value + "'",
							e);
					// value = null;
					// convertedFilter = null;
					throw new ParameterException(
							"Cannot parse CQL expression '" + value + "'",
							e);
				}
			}
			else {
				value = null;
			}
			return new FilterParameter(
					value,
					convertedFilter);
		}
	}

	public static class FilterParameter
	{
		private String cqlFilterString;
		private Filter filter;

		public FilterParameter(
				String cqlFilterString,
				Filter filter ) {
			super();
			this.cqlFilterString = cqlFilterString;
			this.filter = filter;
		}

		public String getCqlFilterString() {
			return cqlFilterString;
		}

		public void setCqlFilterString(
				String cqlFilterString ) {
			this.cqlFilterString = cqlFilterString;
		}

		public Filter getFilter() {
			return filter;
		}

		public void setFilter(
				Filter filter ) {
			this.filter = filter;
		}

		public String toString() {
			return cqlFilterString;
		}
	}
}