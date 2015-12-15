package gov.ornl.stucco.DBClient;

public class InvalidArgumentException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4782572084189675928L;

	public InvalidArgumentException() {
	}

	public InvalidArgumentException(String message) {
		super(message);
	}

	public InvalidArgumentException(Throwable cause) {
		super(cause);
	}

	public InvalidArgumentException(String message, Throwable cause) {
		super(message, cause);
	}

}
