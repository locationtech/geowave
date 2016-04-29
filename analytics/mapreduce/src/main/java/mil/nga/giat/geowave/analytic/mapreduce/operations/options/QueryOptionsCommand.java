package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import java.util.List;

import com.beust.jcommander.Parameter;

public class QueryOptionsCommand
{

	@Parameter(names = "--auth", description = "The comma-separated list of authorizations used during extract; by default all authorizations are used.")
	private List<String> authorizations;

	@Parameter(names = "--adapters", description = "The comma-separated list of data adapters to query; by default all adapters are used.")
	private List<String> adapterIds = null;

	@Parameter(names = "--index", description = "The specific index to query; by default one is chosen for each adapter.")
	private String indexId = null;

	@Parameter(names = "--fields", description = "The comma-separated set of field names to extract; by default all are extracted.")
	private List<String> fieldIds = null;

	public QueryOptionsCommand() {}

	public List<String> getAuthorizations() {
		return authorizations;
	}

	public void setAuthorizations(
			List<String> authorizations ) {
		this.authorizations = authorizations;
	}

	public List<String> getAdapterIds() {
		return adapterIds;
	}

	public void setAdapterIds(
			List<String> adapterIds ) {
		this.adapterIds = adapterIds;
	}

	public String getIndexId() {
		return indexId;
	}

	public void setIndexId(
			String indexId ) {
		this.indexId = indexId;
	}

	public List<String> getFieldIds() {
		return fieldIds;
	}

	public void setFieldIds(
			List<String> fieldIds ) {
		this.fieldIds = fieldIds;
	}
}
