package mil.nga.giat.geowave.analytic.mapreduce;

import org.apache.hadoop.io.Text;

public class GroupIDText extends
		Text
{

	public void set(
			final String groupID,
			final String id ) {
		super.set((groupID == null ? "##" : groupID) + "," + id);
	}

	public String getGroupID() {
		final String t = toString();
		final String groupID = t.substring(
				0,
				t.indexOf(','));
		return ("##".equals(groupID)) ? null : groupID;
	}

	public String getID() {
		final String t = toString();
		return t.substring(t.indexOf(',') + 1);
	}
}
