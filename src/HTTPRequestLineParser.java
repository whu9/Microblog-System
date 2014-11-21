import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HTTPRequestLineParser {

	public static final String regex = "(?<method>\\w+)\\s(?<uri>\\/[^?]+)(\\?(?<parameter>\\w+=([^&])*(?:&\\w+=[^&]*)*))?\\shttp\\/(?<version>.+)";
	public static final String pararegex = "(?<key>\\w+)=(?<value>[^&]*)";
	public static final String repeatingregex = "&(?<key>\\w*)=(?<value>[^&]*)";
	/**
	 * This method takes as input the Request-Line exactly as it is read from
	 * the socket. It returns a Java object of type HTTPRequestLine containing a
	 * Java representation of the line.
	 *
	 * The signature of this method may be modified to throw exceptions you feel
	 * are appropriate. The parameters and return type may not be modified.
	 *
	 * 
	 * @param line
	 * @return
	 * @throws InvalidHttpRequestException 
	 */
	public static HTTPRequestLine parse(String line) throws InvalidHttpRequestException {
		HTTPRequestLine result = new HTTPRequestLine();
		
		//initializing regular expression
		Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher m;
		try {
			m = p.matcher(line);
		} catch (Exception e) {
			throw new InvalidHttpRequestException();
		}
		
		//If the matcher could not find a match, throw an "invalid http request exception"
		if(!m.find()){
			throw new InvalidHttpRequestException();
		}
		
		//read the method, uri and version part from the string and store into the HTTPRequestLine object
		result.setMethod(m.group("method"));
		result.setUriPath(m.group("uri"));
		result.setHttpVersion(m.group("version"));
		
		//read the parameters part from the string and store if for futher parsing.
		String paraLine = m.group("parameter");

		//If there is a parameter part in the string
		if (paraLine != null) {
			
			//decode url
			try {
				paraLine = java.net.URLDecoder.decode(paraLine, "UTF-8");
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			int start = 0;
			
			//Another regular expression matcher to capture the first pair of keys and values
			Pattern parap = Pattern
					.compile(pararegex, Pattern.CASE_INSENSITIVE);
			Matcher param = parap.matcher(paraLine);
			param.find();
			//store an empty string to the map pair if there is not value group captured after the key
			String value = "";
			if ( param.group("value") != null)
			value = param.group("value");
			result.addParameter(param.group("key"), value);
			start = param.end("value");
			
			//find other key/value pairs by locating "&"
			Pattern repeatingp = Pattern.compile(repeatingregex,
					Pattern.CASE_INSENSITIVE);
			Matcher repeatingm = repeatingp.matcher(paraLine);
			
			//loop to find all valid key/value pairs
			while (repeatingm.find(start)) {
				String rvalue = "";
				if(repeatingm.group("value") != null){
					rvalue = repeatingm.group("value");
				}
				result.addParameter(repeatingm.group("key"),
						rvalue);
				start = repeatingm.end("value");
			}
		}
		return result;

	}

}
