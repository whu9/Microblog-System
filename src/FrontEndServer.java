import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * @author Yi Wang The front end server
 */
public class FrontEndServer {

	// Variables for FE
	// String DBUrl = "";
	String DBUrlShort = "localhost:10021";
	String primaryIP;
	int port = 80;

	ExecutorService executor;
	TweetsData cache = null; // the cache of database storing in the FE

	Logger logger = null;
	FileAppender appender = null;

	public FrontEndServer() {
		executor = Executors.newFixedThreadPool(10);
		init();
	}

	// Set customer port
	public FrontEndServer(int port, String primaryIP) {
		executor = Executors.newFixedThreadPool(50);
		init();
		this.primaryIP = primaryIP;
		this.port = port;
	}

	// initiate variables, if loading
	private void init() {
		logger = LogManager.getLogger("FrontEndLogger");
		try {
			readKey("config/connection.json");
			this.DBUrlShort = "localhost:10021";
		} catch (Exception e) {
			logger.error("Cannot read the configuration file, running using default values");
			this.port = 80;
		}
		cache = new TweetsData("Cache");

	}

	// read port and ip addresses from file
	public void readKey(String path) throws Exception {

		JSONParser parser = new JSONParser();
		JSONObject keys = null;

		keys = (JSONObject) parser.parse(new FileReader(path));

		// specify the port for the current server
		port = Integer.parseInt(keys.get("frontendport").toString());
	}

	// Update the cache of the current front end server
	private void updateCache(String str) throws ParseException {

		JSONParser parser = new JSONParser();
		JSONObject json = new JSONObject();

		json = (JSONObject) parser.parse(str);

		String key = json.get("q").toString();
		cache.deleteTag(key);

		int version = Integer.parseInt(json.get("v").toString());
		JSONArray tweets = (JSONArray) json.get("tweets");

		for (int i = 0; i < tweets.size(); i++) {
			String t = tweets.get(i).toString();
			cache.put(key, t);
		}

		cache.setVersion(key, version);

		logger.info("Cache updated for hashtag: {}, version: {}", key, version);
	}

	protected void start() {

		JSONObject obj = new JSONObject();
		try {
			obj.put("ip", InetAddress.getLocalHost().getHostAddress()
					.toString()
					+ ":" + this.port);
		} catch (UnknownHostException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		ServerConnection conncection = new ServerConnection();
		try {
			conncection.sendConnection("POST", primaryIP,
					"server?type=frontend", obj.toString());
		} catch (IOException | BadResponseException | ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ServerSocket serverSock;
		try {
			logger.info("FE server start on {}", InetAddress.getLocalHost()
					.getHostAddress().toString()
					+ ":" + this.port);
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			serverSock = new ServerSocket(port);
		} catch (IOException e) {
			logger.fatal("Cannot start server on port: {}, exception: {}",
					port, e);
			return;
		}

		// for each connection create a new thread
		while (true) {
			try {
				Socket sock = serverSock.accept();
				executor.execute(new ServerThread(sock));
			} catch (Exception e) {
				logger.error(
						"Cannot create new thread for connection, exception: {}",
						e);
			}
		}
	}

	class ServerThread implements Runnable {

		protected Socket sock;

		// the temporary variable storing tags/tweets for updating cache
		private String ctext;
		private HashSet<String> ctags;

		private PrintWriter out;
		private BufferedReader in;

		HTTPRequestLine request = null;
		String postData = "";

		String header = "HTTP/1.1 500 Internal Server Error";
		String body = null;

		public ServerThread(Socket sock) {
			this.sock = sock;
			ctext = "";
			ctags = new HashSet<String>();
		}

		// ****** Start and finish the process******

		// Read the request from socket, parse it, and store it
		private void parseRequest() throws IOException {

			in = new BufferedReader(
					new InputStreamReader(sock.getInputStream()));
			out = new PrintWriter(sock.getOutputStream());

			// read the first line and use url parser to parse it.
			String line = in.readLine();
			logger.info("Get request: {}", line);
			try {
				request = HTTPRequestLineParser.parse(line);
			} catch (InvalidHttpRequestException e) {
				logger.info("Cannot parse request, got invalid request, send HTTP 400");
				header = "HTTP/1.1 400 Bad Request";
				finish(header, null);
				return;
			}

			// read post body
			int postDataI = -1;
			while ((line = in.readLine()) != null && (line.length() != 0)) {
				if (line.startsWith("Content-Length:")) {
					postDataI = new Integer(
							line.substring(
									line.indexOf("Content-Length:") + 16,
									line.length())).intValue();
				}
			}

			if (postDataI > 0) {
				char[] charArray = new char[postDataI];
				in.read(charArray, 0, postDataI);
				postData = new String(charArray);
			}
		}

		// method use to finish the response close socket at the end of the
		// process;
		private void finish(String header, String body) throws IOException {
			out.println(header);
			out.println();
			if (body != null)
				out.println(body);
			out.flush();
			in.close();
			out.close();
			sock.close();
			logger.info("Send response '{}', body '{}' to client", header, body);
		}

		// *********** Handling request ************
		private void doPostTweet() {
			try {
				postToDS(parseBody(postData).toString());
			} catch (ParseException e) {
				logger.error("Could not parse body post body, send 400 to Client");
				return;
			}
			// catch bad response from DB, send 500 to client
			catch (Exception e) {
				logger.error("Got Bad Request response from DB, sending 500 to client");
				header = "HTTP/1.1 500 Internal Server Error";
				return;
			}
			storeToCache(); // store the current tweet to cache
			logger.info("Posted data to DB, Stored to Cache, return 201");
			header = "HTTP/1.1 201 Created";
		}

		private void doPostServer() {
			JSONObject obj = new JSONObject();
			JSONParser p = new JSONParser();
			try {
				obj = (JSONObject) p.parse(postData);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			primaryIP = obj.get("ip").toString();
			logger.info("Setting primary ip to : {}", primaryIP);
			header = "HTTP/1.1 200 OK";
		}

		private void doGetTweet() {
			// if request does not contains 'q'
			if (request.containsKey("q")) {
				String key = request.getValue("q");
				try {
					// perform a search for the string
					body = search(key);
				} catch (NotFoundException e) {
					// if got 404 from DB, send 404
					logger.info("Data Base returns 404, send 404 to client");
					header = "HTTP/1.1 404 Not Found";
					return;
				} catch (BadResponseException e) {
					// if got bad 400 bad request, send 500 internal
					// error to client
					logger.info("Got Bad Request response from DB, send HTTP 500 to Client");
					header = "HTTP/1.1 500 Internal Server Error";
					return;
				}
				logger.info("Get correct response from server, sending 200 and body to client");
				header = "HTTP/1.1 200 OK";
			} else {
				logger.info("Request does not have a 'q' parameter, send HTTP 400");
				header = "HTTP/1.1 400 Bad Request";
			}
		}

		// ************ Other *************
		private void storeToCache() {
			for (String t : ctags) {
				cache.put(t, ctext);
			}
		}

		// parse the body file get from client tweet post request to a JSON
		// object
		private JSONObject parseBody(String body) throws ParseException,
				BadResponseException {
			JSONParser parser = new JSONParser();
			JSONObject json = new JSONObject();
			json = (JSONObject) parser.parse(body);
			String text;
			try {
				text = json.get("text").toString();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				throw new BadResponseException();
			}
			ctext = text;
			JSONObject result = new JSONObject();
			result.put("tweet", text);
			final String regex = "#([^\\s]+)";
			Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(text);

			JSONArray tags = new JSONArray();
			int start = 0;
			while (matcher.find(start)) {
				String tag = matcher.group(1);
				tags.add(tag);
				ctags.add(tag);
				start = matcher.end(1);
			}
			result.put("hashtags", tags);
			return result;
		}

		// post a tweet to data server
		private String postToDS(String urlParameters)
				throws BadResponseException {

			ServerConnection connection = new ServerConnection();
			try {
				connection.sendConnection("POST", primaryIP, "tweets",
						urlParameters);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (BadResponseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (!connection.getHeader().equals("HTTP/1.1 201 Created")) {
				throw new BadResponseException(connection.getHeader());
			}
			return connection.getBody();

		}

		// first search cache for hashtag, then compare version to DataServer.
		// If out of date, retrieve data from dataserver, return to user and
		// update cache, otherwise, retrieve data from cache
		private String search(String key) throws BadResponseException,
				NotFoundException {

			ServerConnection con = new ServerConnection();
			int version = 0;
			if (cache.contains(key)) {
				version = cache.getVersion(key);
				logger.info("Found hashtag: {} entry in cache, version: {}",
						key, version);
			}
			try {
				con.sendConnection("GET", primaryIP, "tweets?q=" + key + "&v="
						+ String.valueOf(version), null);
			} catch (IOException | ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			if (con.getHeader().startsWith("HTTP/1.1 304")) {
				logger.info("Data server returns HTTP 304, fetch data from cache");
				return cache.getClientJson(key).toString();
			} else if (con.getHeader().equals("HTTP/1.1 404 Not Found")) {
				throw new NotFoundException(header);
			} else if (!con.getHeader().equals("HTTP/1.1 200 OK")) {
				throw new BadResponseException(header);
			}
			logger.info("Data server returns HTTP 200, retriving data from Data Server");
			try {
				updateCache(con.getBody().toString());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return cache.getClientJson(key).toString();
		}

		public void run() {

			logger.info("Connection accepted, new thread generated: {}", Thread
					.currentThread().getName());

			try {
				// parse the request from client and store them
				parseRequest();

				// handling request according to request method
				switch (request.getMethod()) {
				case "POST":
					// if end point is not tweets, return 404
					if (request.getUriPath().equals("/tweets")) {
						doPostTweet();
					} else if (request.getUriPath().equals("/server")) {
						doPostServer();
					} else {
						logger.info("Got unknown end point, sending 404");
						header = "HTTP/1.1 404 Not Found";
					}
					break;
				case "GET":
					// if end point is not tweets, return 404
					if (request.getUriPath().equals("/tweets")) {
						doGetTweet();
					} else {
						logger.info("Request does not match \"tweets\" or does not have parameter q, sending Bad Request response to client");
						header = "HTTP/1.1 404 Not Found";
					}
					break;
				default:
					// if method is not GET or POST, return 405
					logger.info("Request is unsupported method, send 405 to client");
					header = "HTTP/1.1 405 Method Not Allowed";
					break;
				}

				// send response to client
				finish(header, body);

			} catch (IOException e) {
				logger.error("Cannot send response, exception: {}", e);
			}
		}
	}

	public static void main(String args[]) {
		FrontEndServer fs;
		fs = new FrontEndServer(Integer.parseInt(args[0]), args[1]);
		fs.start();
	}
}