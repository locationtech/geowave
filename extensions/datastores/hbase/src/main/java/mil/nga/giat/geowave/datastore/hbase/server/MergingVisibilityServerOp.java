package mil.nga.giat.geowave.datastore.hbase.server;

public class MergingVisibilityServerOp extends
		MergingServerOp
{

	@Override
	protected boolean includeTags() {
		return false;
	}

}