package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

public interface CompressingCluster<INTYPE, OUTTYPE> extends
		Cluster<INTYPE>
{
	public OUTTYPE get();
}