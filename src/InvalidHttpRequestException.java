
public class InvalidHttpRequestException extends Exception {
	public InvalidHttpRequestException() {
		super("Invalid Url Path Request");
	}

	public InvalidHttpRequestException(String msg) {
		super(msg);
	}
}