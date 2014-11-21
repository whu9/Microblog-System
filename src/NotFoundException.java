
public class NotFoundException extends Exception {
	public NotFoundException() {
		super("Invalid HTTP Request");
	}

	public NotFoundException(String msg) {
		super(msg);
	}
}