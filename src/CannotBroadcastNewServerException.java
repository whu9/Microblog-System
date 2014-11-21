
public class CannotBroadcastNewServerException extends Exception {
	public CannotBroadcastNewServerException() {
		super("Invalid HTTP Request");
	}

	public CannotBroadcastNewServerException(String msg) {
		super(msg);
	}
}