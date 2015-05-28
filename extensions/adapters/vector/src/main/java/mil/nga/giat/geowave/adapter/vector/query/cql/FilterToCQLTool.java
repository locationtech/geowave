package mil.nga.giat.geowave.adapter.vector.query.cql;

import org.opengis.filter.Filter;

public class FilterToCQLTool
{
	public static String toCQL(
			Filter filter ) {

		FilterToECQLExtension toCQL = new FilterToECQLExtension();

		StringBuilder output = (StringBuilder) filter.accept(
				toCQL,
				new StringBuilder());

		return output.toString();
	}
}
