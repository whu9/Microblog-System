
public class BadResponseException extends Exception {
	public BadResponseException() {
		super("Invalid HTTP Request");
	}

	public BadResponseException(String msg) {
		super(msg);
	}
}