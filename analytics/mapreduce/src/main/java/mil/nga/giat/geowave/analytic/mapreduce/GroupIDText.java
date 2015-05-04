package mil.nga.giat.geowave.analytic.mapreduce;

import org.apache.hadoop.io.Text;

public class GroupIDText extends
		Text
{

	public void set(
			String groupID,
			String id ) {
		super.set((groupID == null ? "##" : groupID) + "," + id);
	}

	public String getGroupID() {
		String t = toString();
		String groupID = t.substring(
				0,
				t.indexOf(','));
		return ("##".equals(groupID)) ? null : groupID;
	}

	public String getID() {
		String t = toString();
		return t.substring(t.indexOf(',') + 1);
	}
}
