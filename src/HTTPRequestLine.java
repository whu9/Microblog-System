import java.util.HashMap;
import java.util.Set;

/**
 * HTTPRequestLine is a data structure that stores a Java representation of the
 * parsed Request-Line.
 **/
public class HTTPRequestLine {

	private HTTPConstants.HTTPMethod method;
	private String uripath;
	private HashMap<String, String> parameters;
	private String httpversion;

	/*
	 * You are expected to add appropriate constructors/getters/setters to
	 * access and update the data in this class.
	 */
	public HTTPRequestLine(){
		uripath = "";
		parameters = new HashMap<String, String>();
		httpversion = "";
	}
	
	public void setMethod(String method) throws InvalidHttpRequestException {
		switch (method) {
		case "GET":
			this.method = this.method.GET;
			break;
		case "OPTIONS":
			this.method = this.method.OPTIONS;
			break;
		case "CONNECT":
			this.method = this.method.CONNECT;
			break;
		case "DELETE":
			this.method = this.method.DELETE;
			break;
		case "HEAD":
			this.method = this.method.HEAD;
			break;
		case "POST":
			this.method = this.method.POST;
			break;
		case "PUT":
			this.method = this.method.PUT;
			break;
		case "TRACE":
			this.method = this.method.TRACE;
			break;
		default:
			throw new InvalidHttpRequestException("Invalid method");
		}
	}

	public String getMethod() {
		return this.method.toString();
	}

	public void setUriPath(String uri) {
		this.uripath = uri;
	}

	public String getUriPath() {
		return this.uripath;
	}

	public void addParameter(String key, String value) {
		parameters.put(key, value);
	}

	public Set<String> getKeySet(){
		return parameters.keySet();
	}
	public boolean containsKey(String key) {
		return parameters.containsKey(key);
	}

	public String getValue(String key) {
		return parameters.get(key);
	}

	public void setHttpVersion(String version) {
		this.httpversion = version;
	}
	
	public String getHttpVersion(){
		return this.httpversion;
	}
	


}
