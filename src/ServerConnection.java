import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.simple.parser.ParseException;

/**
 * 
 * @author Yi Wang
 * A helper class used to send HTTP requests to other servers inside this system.
 *
 */
public class ServerConnection {
	
	private String header;
	private String body;

	public ServerConnection() {
		header = null;
		body = null;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public String getHeader() {
		return this.header;
	}

	public String getBody() {
		return this.body;
	}

	/**
	 * send HttpURLConnection to the specified IP, store response headers and body.
	 * @param method the HTTP method.
	 * @param ip the target ip.
	 * @param param the request parameters.
	 * @param body the body of request.
	 * @throws IOException
	 * @throws BadResponseException
	 * @throws ParseException
	 */
	public void sendConnection(String method, String ip, String param,
			String body) throws IOException, BadResponseException,
			ParseException {
		URL url = new URL("http://" + ip + "/" + param);
//		System.out.println(url.toString());
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setConnectTimeout(10 * 1000);
		connection.setRequestMethod(method);

		if (!method.equals("GET")) {
			connection.setRequestProperty("Content-Type", "application/json");
			connection.setRequestProperty("Content-Length",
					"" + Integer.toString(body.getBytes().length));

			connection.setUseCaches(false);
			connection.setDoInput(true);
			connection.setDoOutput(true);

			// Send request
			DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
			wr.writeBytes(body);
			wr.flush();
			wr.close();
		}

		String headers = "";
		for (int i = 0;; i++) {
			String header = connection.getHeaderField(i);
			if (header == null) {
				break;
			}
			headers = headers + header;
		}

		this.header = headers;

		// Get Response
		InputStream is = connection.getInputStream();
		BufferedReader rd = new BufferedReader(new InputStreamReader(is));
		String line;
		StringBuffer response = new StringBuffer();
		while ((line = rd.readLine()) != null) {
			response.append(line);
			response.append('\n');
		}
		rd.close();
		if (connection != null) {
			connection.disconnect();
		}
		this.body = response.toString();
	}
}
