
public class VersionNotMatchException extends Exception {
	public VersionNotMatchException() {
		super("Invalid HTTP Request");
	}

	public VersionNotMatchException(String msg) {
		super(msg);
	}
}