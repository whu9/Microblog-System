
public class InvalidUrlPathException extends Exception {
	public InvalidUrlPathException() {
		super("Invalid HTTP Request");
	}

	public InvalidUrlPathException(String msg) {
		super(msg);
	}
}