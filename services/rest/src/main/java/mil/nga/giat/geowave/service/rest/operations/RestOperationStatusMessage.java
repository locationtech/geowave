package mil.nga.giat.geowave.service.rest.operations;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class RestOperationStatusMessage
{
	public enum StatusType {
		NONE,
		STARTED,
		RUNNING,
		COMPLETE,
		ERROR
	};

	/**
	 * A REST Operation message that is returned via JSON
	 */
	@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
	public StatusType status;

	@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
	public String message;

	@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
	public Object data;

	public RestOperationStatusMessage() {
		status = StatusType.NONE;
		message = "";
		data = null;
	}
}
