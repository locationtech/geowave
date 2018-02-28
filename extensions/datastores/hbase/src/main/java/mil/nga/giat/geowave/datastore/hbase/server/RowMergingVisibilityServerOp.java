package mil.nga.giat.geowave.datastore.hbase.server;

public class RowMergingVisibilityServerOp extends
		RowMergingServerOp
{

	@Override
	protected boolean includeTags() {
		return false;
	}

}
