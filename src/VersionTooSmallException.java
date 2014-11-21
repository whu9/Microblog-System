
public class VersionTooSmallException extends Exception {
	public VersionTooSmallException() {
		super("Invalid HTTP Request");
	}

	public VersionTooSmallException(String msg) {
		super(msg);
	}
}