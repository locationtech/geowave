package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A DB Scan cluster Item
 */
public class ClusterItem
{
	private final String id;
	private final Geometry geometry;
	private final long count;

	public ClusterItem(
			final String id,
			final Geometry geometry,
			final long count ) {
		super();
		this.id = id;
		this.geometry = geometry;
		this.count = count;
	}

	protected String getId() {
		return id;
	}

	protected Geometry getGeometry() {
		return geometry;
	}

	protected long getCount() {
		return count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		ClusterItem other = (ClusterItem) obj;
		if (id == null) {
			if (other.id != null) return false;
		}
		else if (!id.equals(other.id)) return false;
		return true;
	}

	@Override
	public String toString() {
		return "ClusterItem [id=" + id + ", geometry=" + geometry + ", count=" + count + "]";
	}

}
