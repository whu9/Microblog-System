
public class ElectionFailException extends Exception {
	public ElectionFailException() {
		super("Invalid HTTP Request");
	}

	public ElectionFailException(String msg) {
		super(msg);
	}
}