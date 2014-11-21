import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * The instance of a back end server
 * 
 * @author Yi Wang
 *
 */
public class DataServer {

	// server info
	int port = 80;
	int id = -1;
	boolean isPrimary = false;
	int primaryId = -1;

	// during election process or not
	boolean election = false;

	ExecutorService executor;
	ServerSocket serverSock;

	// The data and list of servers in the system
	TweetsData data = null;
	ServerList serverList = null;
	FrontEndList frontEndList = null;

	// log4j logger
	Logger logger = null;
	FileAppender appender = null;

	// used for demo
	boolean stop = false;
	boolean sleep = false;

	/**
	 * The constructor for Primary servers. primaryId will be set to current id.
	 * 
	 * @param port
	 *            the port at which the server runs.
	 * @param id
	 *            the id of current server.
	 */
	public DataServer(String port, String id) {

		this.port = Integer.parseInt(port);
		this.id = Integer.parseInt(id);
		this.isPrimary = true;
		this.primaryId = Integer.parseInt(id);

		init();

		try {
			serverList.addServer(this.id, InetAddress.getLocalHost()
					.getHostAddress().toString()
					+ ":" + this.port);
		} catch (UnknownHostException e) {
			System.exit(1);
		}

		executor = Executors.newFixedThreadPool(50);
	}

	/**
	 * The constructor for secondary servers. primaryId and ip need to be given.
	 * The server will contact the primary server using the ip address given
	 * notify that it wants to join the system. It will update its current
	 * server list and database from the primary.
	 * 
	 * @param port
	 *            the port at which the server runs.
	 * @param id
	 *            the id of this server.
	 * @param pId
	 *            the id of Primary server.
	 * @param ipAddress
	 *            the IP address of primary server.
	 */
	public DataServer(String port, String id, String pId, String ipAddress) {

		this.port = Integer.parseInt(port);
		this.id = Integer.parseInt(id);
		this.isPrimary = false;
		this.primaryId = Integer.parseInt(pId);

		init();

		try {
			serverList.addServer(this.id, InetAddress.getLocalHost()
					.getHostAddress().toString()
					+ ":" + this.port);
		} catch (UnknownHostException e) {
			System.exit(1);
		}

		executor = Executors.newFixedThreadPool(50);

		try {
			sendAddNotification(ipAddress);
		} catch (Exception e) {
			logger.fatal("Cannot add myself to the system, error: {}", e);
		}
	}

	/**
	 * initiate some data.
	 */
	private void init() {
		stop = false;
		logger = LogManager.getLogger("DataBaseLogger");
		System.out.println("=========== Server " + String.valueOf(id)
				+ " ===========");
		logger.info("Logger initialized, Data Server #{} initiating.", id);
		data = new TweetsData("Database");
		serverList = new ServerList();
		frontEndList = new FrontEndList();
		ServerSocket serverSock = null;
	}

	/**
	 * Send notification to the current primary server asking for joining the
	 * system
	 * 
	 * @param targetIp
	 *            the IP of the current primary server.
	 */
	private void sendAddNotification(String targetIp) throws IOException,
			BadResponseException, ParseException {

		// Put info about itself in a Json
		JSONObject json = new JSONObject();
		json.put("id", id);
		json.put("ip", InetAddress.getLocalHost().getHostAddress().toString()
				+ ":" + this.port);
		String jStr = json.toString();

		// Connect to primary and post add request
		ServerConnection connection = new ServerConnection();
		connection.sendConnection("POST", targetIp, "server?type=new", jStr);
		if (!connection.getHeader().equals("HTTP/1.1 200 OK")) {
			throw new BadResponseException(connection.getHeader());
		}

		// Parser response body and update local server list
		JSONObject obj = new JSONObject();
		JSONParser parser = new JSONParser();
		obj = (JSONObject) parser.parse(connection.getBody());

		serverList.update(obj);
		logger.info("Successfully added to the system, updated local list: {}",
				serverList.getListJson().toString());

		// Copy current tweet data from primary
		try {
			copyData();
		} catch (VersionNotMatchException e) {
			logger.fatal("Could not copy Tweet Data from Primary, start up abort");
			System.exit(1);
		}

		updateFrontEndList();
	}

	/**
	 * Send request to Primary and request for copy the entire Tweet data
	 * 
	 * @throws VersionNotMatchException
	 *             Throw when data base version not match primary after put all
	 *             tweets into database
	 */
	private void copyData() throws VersionNotMatchException {

		// Send request to primary
		ServerConnection connection = new ServerConnection();
		try {
			connection.sendConnection("GET", serverList.getIP(primaryId),
					"data", "");
		} catch (IOException | BadResponseException | ParseException e) {
			logger.fatal("Cannot copy Tweets data from primary, start up abort");
			System.exit(1);
		}

		// parse the request body update local data base
		JSONObject obj = new JSONObject();
		JSONParser parser = new JSONParser();
		try {
			obj = (JSONObject) parser.parse(connection.getBody());
		} catch (ParseException e) {
			logger.fatal("Cannot parse copied Tweets data from primary, start up abort");
			System.exit(1);
		}
		JSONArray arr = (JSONArray) obj.get("data");

		// For each tweet, put into data base
		for (int i = 0; i < arr.size(); i++) {
			JSONObject tagData = (JSONObject) arr.get(i);
			String tag = (String) tagData.get("q");
			JSONArray arr2 = (JSONArray) tagData.get("tweets");
			for (int j = 0; j < arr2.size(); j++) {
				String tweet = (String) arr2.get(j);
				data.put(tag, tweet);
			}
		}

		// Check if data version of primary server matchs local version
		int primaryVersion = Integer.parseInt(obj.get("version").toString());
		if (primaryVersion != data.getDBVersion()) {
			logger.fatal("Version not match after apply patch, start up abort");
			System.exit(1);
		}

		logger.info("Successfully copied Tweets data from Primary server");
	}

	/**
	 * Update the missing POST from the target server
	 * 
	 * @param tempID
	 *            the ID of target server
	 */
	private void updateData(int tempID) {

		ServerConnection con = new ServerConnection();
		try {
			con.sendConnection("GET", serverList.getIP(tempID),
					"update?version=" + data.getDBVersion(), "");
		} catch (IOException | BadResponseException | ParseException e) {
			logger.error("Request data update from server {} error: {}",
					tempID, e);
		}

		// parse body
		JSONObject obj = new JSONObject();
		JSONParser parser = new JSONParser();
		try {
			obj = (JSONObject) parser.parse(con.getBody());
		} catch (ParseException e) {
			logger.error("Update data error: Could not parse response body");
		}

		int pVersion = Integer.parseInt(obj.get("version").toString());

		// for each missing version, put the tweet to local database
		for (int i = data.getDBVersion() + 1; i <= pVersion; i++) {
			JSONObject log = (JSONObject) obj.get(String.valueOf(i));
			logger.info("updating log {} : {}", i, log.toString());
			data.put(log.get("tag").toString(), log.get("tweet").toString());
		}
		logger.info("Local data updated finished, current data: {}\n",
				data.getDBJson());
	}

	/**
	 * Called at start up, Send request to Primary server for current front end
	 * server List.
	 */
	private void updateFrontEndList() {

		// send the request to primary
		ServerConnection con = new ServerConnection();
		try {
			con.sendConnection("GET", serverList.getIP(primaryId),
					"server?type=frontend", null);
		} catch (IOException | BadResponseException | ParseException e) {
			logger.fatal("Cannot update front end list, start up abort");
			System.exit(1);
		}

		// parse the response body
		JSONObject obj = new JSONObject();
		JSONParser parser = new JSONParser();
		try {
			obj = (JSONObject) parser.parse(con.getBody());
		} catch (ParseException e) {
			logger.fatal("Cannot parse front end list from primary, start up abort");
			System.exit(1);
		}
		JSONArray arr = new JSONArray();
		arr = (JSONArray) obj.get("fe");

		// put each server ip into local list
		for (int i = 0; i < arr.size(); i++) {
			String ip = arr.get(i).toString();
			frontEndList.addServer(ip);
		}

		logger.info(
				"Successfully updated front end server list, current list: {}",
				frontEndList.getJson().toString());
	}

	/**
	 * The discover service which will be called every 5 seconds. If this server
	 * is a secondary and it found a failure of a secondary server, it will
	 * report to primary; if it found a failure of the primary, it will initiate
	 * the election process; if this server is a primary and found a failure of
	 * secondary, it will delete if from list and notify all other servers
	 * 
	 * 
	 * @param tempId
	 *            The IP of the server to contact.
	 */
	private void discover(int tempId) {

		// try to connection server
		ServerConnection connection = new ServerConnection();
		try {
			connection.sendConnection("GET", serverList.getIP(tempId),
					"server?type=alive", "");
		} catch (Exception e) {
			// If connection fails, put it to "fail list". If a same id has been
			// put to this list for 3 times, initiate failure process
			if (serverList.addFailure(tempId)) {
				logger.error(
						"Cannot connect to server {} for 3 times, initiate failure process",
						tempId);
				executor.execute(new FailureHandlerThread(tempId));
			} else {
				logger.error("Cannot connect to server {}", tempId);
			}
		}
	}

	protected void start() {

		try {
			serverSock = new ServerSocket(port);
		} catch (Exception e) {
			logger.fatal("Cannot start server on port: {}, exception: {}",
					port, e);
			return;
		}

		/*
		 * Use Timer to start discover service every 5 second
		 */
		TimerTask discoveryTask = new TimerTask() {
			public void run() {
				for (int tempId : serverList.getIdSet()) {
					if (tempId != id) {
						discover(tempId);
					}
				}
			}
		};

		Timer timer = new Timer(true);
		timer.scheduleAtFixedRate(discoveryTask, 5 * 1000, 5 * 1000);
		logger.info("Discovery service for all back end server started");

		logger.info("Data server start on {}", this.serverList.getIP(this.id));
		System.out.println();

		while (!stop) {
			try {
				Socket sock = serverSock.accept();
				executor.execute(new ServerThread(sock));
			} catch (Exception e) {
				logger.error(
						"#{}: Cannot create new thread for connection, exception: {}",
						id, e);
			}
		}
	}

	// For demo use, stop a server
	public void stop() {
		stop = true;
	}

	/**
	 * The thread used to handle failure detection.
	 * 
	 * @author Yi Wang
	 *
	 */
	class FailureHandlerThread implements Runnable {

		int failId = -1;

		public FailureHandlerThread(int failId) {
			this.failId = failId;
		}

		/**
		 * Send message to all secondaries notify the failure of the specified
		 * server. Used when primary determines that a secondary is failed.
		 * 
		 * @param tempId
		 *            the ID of the failed server.
		 */
		private void broadCastFailure(int tempId) {

			serverList.deleteServer(tempId);
			logger.debug(
					"Primary: Failure detected. Broadcasting failure message of {}",
					tempId);

			// detect if broadcasting failure fails
			boolean success = true;
			for (int sendId : serverList.getIdSet()) {
				if (tempId != sendId && sendId != primaryId) {
					ServerConnection connection = new ServerConnection();
					try {
						connection.sendConnection("POST",
								serverList.getIP(sendId),
								"server?type=failure&id=" + tempId, "");
					} catch (IOException | BadResponseException
							| ParseException e) {
						success = false;
					}
					if (!connection.getHeader().equals("HTTP/1.1 200 OK")) {
						logger.error(connection.toString(),
								connection.getHeader());
						success = false;
					}
				}
			}
			if (success)
				logger.debug("Successfully broadcasted failure message of {}",
						tempId);
			else {
				logger.error("cannot broadcast failure, re-initiate process");
				broadCastFailure(tempId);
			}
		}

		/**
		 * Called when secondary detects a failure. If it is a failure of
		 * secondary, report to primary, otherwise start the election process.
		 * 
		 * @param tempId
		 */
		private void reportFailure(int tempId) {

			// If it is a failure of primary, trigger election process
			if (tempId == primaryId) {
				/*
				 * If election process is ongoing in this server, do nothing, /*
				 * other wise start the process.
				 */
				if (election) {
					logger.debug("Detect primary failure, still waiting for election result\n");
				} else if (!election) {
					logger.debug("Detected Primary failure, start election process");
					election();
				}
			}

			// if it is secondary, report to primary
			else {
				logger.debug(
						"Secondary{} : detected failure of {}, report to primary",
						id, tempId);
				ServerConnection connection = new ServerConnection();
				try {
					connection.sendConnection("POST",
							serverList.getIP(primaryId),
							"server?type=failure&id=" + tempId, "");
				} catch (IOException | BadResponseException | ParseException e) {
					logger.error("Report failed, error: {}", e);
				}
				if (!connection.getHeader().equals("HTTP/1.1 202 Accepted")) {
					logger.error("Report failed, primary returns {}",
							connection.getHeader());
				} else {
					logger.info("Reported to primary, waiting for response\n");
				}
			}
		}

		/**
		 * The election process using bully algorithm.
		 */
		private void election() {

			// Whether set this server to be primary or not.
			boolean setPrimary = true;

			/*
			 * for each ID in the server list that is smaller than the ID of
			 * this server, send election request. If the smaller ID server
			 * returns 202, wait for the result and do nothing, if all smaller
			 * ID server did not return, elect this server to be primary.
			 */
			for (int tempId : serverList.getIdSet()) {
				if (tempId < id) {
					ServerConnection connection = new ServerConnection();

					try {
						connection.sendConnection("GET",
								serverList.getIP(tempId), "server?type=elect",
								"");
					} catch (IOException | BadResponseException
							| ParseException e) {
						logger.debug("Get bad response from BE {} : {}",
								tempId, e);
					}

					if (connection.getHeader().equals("HTTP/1.1 202 Accepted")) {
						setPrimary = false;
						logger.debug("Election process found higher BE, waiting for result\n");
					} else {
						logger.debug("Get bad response from BE {} : {}",
								tempId, connection.getHeader());
					}
				} else
					// If the tempId is greater of equal to the current ID,
					// break the loop.
					break;
			}

			/*
			 * If election result is this server, set it to be primary and send
			 * notification to all others. When secondaries receive election
			 * result, they will return its current data version. update itself
			 * from the secondary with latest data, broadcast data version to
			 * secondaries agian. When Secondaries receive version broadcast, it
			 * will request for update from the new primary if its data is not
			 * up-to-date
			 */
			if (setPrimary) {

				logger.debug(
						"Election process elected myself to be primary, broadcasting results",
						id);

				serverList.deleteServer(primaryId);
				primaryId = id;
				isPrimary = true;

				int maxVersion = -1;
				int maxId = -1;
				for (int tempId : serverList.getIdSet()) {
					if (tempId != id) {
						ServerConnection connection = new ServerConnection();
						try {
							connection.sendConnection(
									"POST",
									serverList.getIP(tempId),
									"server?type=elect&id="
											+ String.valueOf(id), "");
						} catch (IOException | BadResponseException
								| ParseException e) {
							logger.debug(
									"Send election result message to {} fails, exception: {}",
									tempId, e);
						}
						if (!connection.getHeader().equals("HTTP/1.1 200 OK")) {
							logger.debug(
									"Send election result message to server {} fails, get bad response: {}",
									tempId, connection.getHeader());
						}

						JSONParser parser = new JSONParser();
						JSONObject obj = new JSONObject();
						try {
							obj = (JSONObject) parser.parse(connection
									.getBody());
						} catch (ParseException e) {
							logger.debug("Cound not parse response body");
						}
						int version = Integer.parseInt(obj.get("version")
								.toString());
						if (version > maxVersion) {
							maxVersion = version;
							maxId = tempId;
						}
					}
				}

				if (maxVersion <= data.getDBVersion()) {
					logger.info(
							"#{}: election process complete, comparing data version finished, myself has newest data\n",
							id);
				} else {
					logger.info(
							"#{}: election process complete, found higher data version {} at {}, request data update",
							id, maxVersion, maxId);
					updateData(maxId);
				}

				for (int tempId : serverList.getIdSet()) {
					if (tempId != id) {
						ServerConnection con = new ServerConnection();
						try {
							con.sendConnection("POST",
									serverList.getIP(tempId), "update?version="
											+ data.getDBVersion(), "");
						} catch (IOException | BadResponseException
								| ParseException e) {
							logger.info(
									"Could not send update message to server {}",
									tempId);
						}
					}
				}

				for (String ip : frontEndList.getList()) {
					JSONObject obj = new JSONObject();
					obj.put("ip", serverList.getIP(id));
					obj.put("version", data.getDBVersion());
					ServerConnection con = new ServerConnection();
					try {
						con.sendConnection("POST", ip, "server", obj.toString());
					} catch (IOException | BadResponseException
							| ParseException e) {
						logger.info(
								"Cound not inform front end server {} for election result",
								ip);
					}
				}
				// The election process is finished, set it to false.
				election = false;
			}
		}

		@Override
		public void run() {

			System.out.println("=========== Server " + String.valueOf(id)
					+ " ===========");

			/*
			 * Inform server list that this failure is being handled so that
			 * failure handling process will not be triggered again
			 */
			serverList.handlingFailure(failId);

			if (isPrimary) {
				broadCastFailure(failId);
			} else {
				reportFailure(failId);
			}
		}
	}

	/**
	 * The main server thread instance. Handles all incoming HTTP request.
	 * 
	 * @author Peter
	 *
	 */
	class ServerThread implements Runnable {

		protected Socket sock;

		BufferedReader in;
		PrintWriter out;

		String header = null;
		String body = null;

		HTTPRequestLine request = null;
		String postData = "";

		public ServerThread(Socket sock) {
			this.sock = sock;
		}

		/*
		 * *********************** Start and finish **************************
		 * Helper methods used to parse request and send response.
		 */

		private void parseRequest() throws IOException,
				InvalidHttpRequestException {

			in = new BufferedReader(
					new InputStreamReader(sock.getInputStream()));
			out = new PrintWriter(sock.getOutputStream());

			// read the first line and use url parser to parse it.
			String line = in.readLine();

			if (!line.startsWith("GET /server?type=alive")) {
				System.out.println("=========== Server " + String.valueOf(id)
						+ " ===========");
				logger.info("Get request: {}", line);
			}

			request = HTTPRequestLineParser.parse(line);

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
			if (postData.length() > 0)
				logger.info("Get request body: {}", postData);
		}

		/**
		 * Method use to finish request handling process. Send the response and
		 * close the socket.
		 * 
		 * @param header
		 *            the response header.
		 * @param body
		 *            the response body.
		 * @throws IOException
		 */
		private void finish(String header, String body) throws IOException {
			out.println(header);
			out.println();
			if (body != null)
				out.println(body);
			out.flush();
			in.close();
			out.close();
			sock.close();
			if (!request.getUriPath().equals("/server")
					|| !request.getValue("type").equals("alive"))
				logger.info("Send response '{}', body '{}' to client\n",
						header, body);
		}

		/*
		 * *********************** Handling requests **************************
		 * Methods used to handle different requests
		 */
		private void doPostTweet() {

			logger.info("Handling POST tweet request");

			if (isPrimary) {
				try {
					addTweet();
				} catch (ParseException | NullPointerException e) {
					logger.info(
							"could not parse body '{}', send 400 to client, exception: {}",
							postData, e);
					e.printStackTrace();
					header = "HTTP/1.1 400 Bad Request";
					return;
				}

			} else {
				try {
					addTweetSecondary();
				} catch (ParseException | NullPointerException
						| VersionNotMatchException e) {
					logger.info(
							"could not parse body '{}', send 400 to client, exception: {}",
							postData, e);
					e.printStackTrace();
					header = "HTTP/1.1 400 Bad Request";
					return;
				}
			}
			logger.info("Posted to Database, return 200 to FE");
			header = "HTTP/1.1 201 Created";
		}

		private void doGetTweet() {
			

			logger.info("Handling GET tweet request");
			if (request.containsKey("v") && request.containsKey("q")) {
				String key = request.getValue("q");
				// check if hashtag in the database
				if (!data.contains(key)) {
					logger.info("Hashtag: '{}' not found, return 404 to FE");
					header = "HTTP/1.1 404 Not Found";
				} else if (data.getVersion(key) == Integer.parseInt(request
						.getValue("v"))) {
					logger.info("Hashtag: '{}' found with same version, return 304 to FE");
					header = "HTTP/1.1 304 Not Modified";
				} else {
					logger.info("Hashtag: '{}' found with higher version, return 200 and body to FE");
					header = "HTTP/1.1 200 OK";
					body = data.getJson(key).toString();
				}
			} else {
				logger.info("Request does not contain both 'q' and 'v', return 400 to FE");
				header = "HTTP/1.1 400 Bad Request";
			}
		}

		private void doPostServer() throws Exception {
			if (request.containsKey("type")) {

				/*
				 * If got request type new and this is a primary server, send
				 * new server message to all secondaries.
				 */
				if (request.getValue("type").equals("new") && isPrimary) {

					logger.info("Handling POST new secondary joining");
					
					JSONParser parser = new JSONParser();
					JSONObject json = null;

					try {
						json = (JSONObject) parser.parse(postData);
					} catch (ParseException e) {
						header = "HTTP/1.1 400 Bad Requset";
						return;
					}

					int newId = Integer.parseInt(json.get("id").toString());
					String newIp = json.get("ip").toString();
					serverList.addServer(newId, newIp);

					logger.info(
							"New server added to server list, current list: {}",
							serverList.getListJson().toString());
					header = "HTTP/1.1 200 OK";
					// Return the current list of servers
					body = serverList.getListJson().toString();

					boolean fails = false;
					for (int tempId : serverList.getIdSet()) {
						if (tempId != id && tempId != newId) {
							try {
								sendUpdateNotification(
										serverList.getIP(tempId), newId, newIp);
							} catch (Exception e) {
								logger.error(
										"Broadcasting new server message to {} error: {}",
										tempId, e);
								fails = true;
							}
						}
					}
					if (fails) {
						header = "HTTP/1.1 500 Internal Server Error";
						return;
					}
				}

				/*
				 * If get request type update and this server is a secondary,
				 * update the local list
				 */
				else if (request.getValue("type").equals("update")
						&& !isPrimary) {

					logger.info("Handling POST server list update ");
					
					JSONParser parser = new JSONParser();
					JSONObject json = null;

					try {
						json = (JSONObject) parser.parse(postData);
					} catch (ParseException e) {
						header = "HTTP/1.1 400 Bad Requset";
						return;
					}

					int newId = Integer.parseInt(json.get("id").toString());
					String newIp = json.get("ip").toString();

					serverList.addServer(newId, newIp);
					header = "HTTP/1.1 200 OK";

					logger.info(
							"New server added to server list, current list: {}",
							serverList.getListJson().toString());
				}

				/*
				 * Handles request type failure. If primary, try to contact the
				 * server. If contact fails, add the ID received to failure
				 * list. If secondary, delete the server from local list
				 */
				else if (request.getValue("type").equals("failure")) {
					
					if (!isPrimary) {
						logger.info("Handling POST server failure");
						serverList.deleteServer(Integer.parseInt(request
								.getValue("id")));
						header = "HTTP/1.1 200 OK";

						logger.debug(
								"Recieved failure broadcast from primary, secondary server {} deleted from list",
								Integer.parseInt(request.getValue("id")));
					} else {

						logger.info("Handling POST server failure report");
						int tempId = Integer.parseInt(request.getValue("id"));

						if (!serverList.hasFailure(tempId)) {
							ServerConnection connection = new ServerConnection();
							try {
								connection.sendConnection("GET",
										serverList.getIP(tempId),
										"server?type=alive", "");
							} catch (IOException | BadResponseException
									| ParseException e) {
								serverList.addFailure(tempId);
							}
						}

						header = "HTTP/1.1 202 Accepted";

						logger.debug("Recieved failure report from secondary, add to failure list");
					}
				}

				/*
				 * Handles request type elect. If the ID of current server is
				 * smaller than the ID received, returns 403 Forbidden,
				 * otherwise set it to primary.
				 */
				else if (request.getValue("type").equals("elect")) {

					logger.info("Handling POST election request");
					
					if (Integer.parseInt(request.getValue("id")) > id) {
						header = "HTTP/1.1 403 Forbidden";
					} else {
						serverList.deleteServer(primaryId);
						primaryId = Integer.parseInt(request.getValue("id"));
						election = false;

						header = "HTTP/1.1 200 OK";
						JSONObject obj = new JSONObject();
						obj.put("version", data.getDBVersion());
						body = obj.toString();

						logger.debug(
								"Recieved election process result, set {} as new primary",
								primaryId);
					}
				}

				else if (request.getValue("type").equals("frontend")) {
					
					logger.info("Handling POST new front end");
					
					if (isPrimary) {
						for (int tempId : serverList.getIdSet()) {
							if (tempId != id) {
								ServerConnection con = new ServerConnection();
								try {
									con.sendConnection("POST",
											serverList.getIP(tempId),
											"server?type=frontend", postData);
								} catch (IOException | BadResponseException
										| ParseException e) {
									logger.error("Could not send front end list update massege to {}", tempId);
								}
							}
						}
					}

					JSONObject obj = new JSONObject();
					JSONParser parser = new JSONParser();
					try {
						obj = (JSONObject) parser.parse(postData);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					frontEndList.addServer(obj.get("ip").toString());
					logger.info("{}: FE list updated, current list: {}", id,
							frontEndList.getList().toString());
					header = "HTTP/1.1 200 OK";
				}
			} else {
				header = "HTTP/1.1 400 Bad Request";
			}
		}

		private void doGetServer() {
			if (request.containsKey("type")) {
				if (request.getValue("type").equals("alive")) {
					header = "HTTP/1.1 200 OK";
				} else if (request.getValue("type").equals("elect")) {
					logger.info("Handling election GET request from smaller ID");
					header = "HTTP/1.1 202 Accepted";
				} else if (request.getValue("type").equals("frontend")) {
					logger.info("Handling election GET request for FE list");
					header = "HTTP/1.1 200 OK";
					body = frontEndList.getJson().toString();
				}
			} else {
				header = "HTTP/1.1 400 Bad Request";
			}
		}

		private void doGetData() {
			header = "HTTP/1.1 200 OK";
			body = data.getDBJson().toString();
		}

		private void doGetUpdate() {
			int diffVersion = Integer.parseInt(request.getValue("version"));
			logger.info("#{}: Recieved Update Data request, diff version {}",
					id, request.getValue("version"));

			JSONObject obj = new JSONObject();
			for (int i = 1; i <= data.getDBVersion() - diffVersion; i++) {
				obj.put(diffVersion + i, data.getLogJson(diffVersion + i));
			}
			obj.put("version", data.getDBVersion());
			header = "HTTP/1.1 200 OK";
			body = obj.toString();
		}

		private void doPostUpdate() {
			int pVersion = Integer.parseInt(request.getValue("version"));
			if (pVersion <= data.getDBVersion()) {
				header = "HTTP/1.1 304 Not Modified";
			} else {
				updateData(primaryId);
				header = "HTTP/1.1 200 OK";
			}
		}

		private void doDeleteTweet() {
			if (request.containsKey("version") && request.containsKey("size")) {
				data.delete(Integer.parseInt(request.getValue("version")),
						Integer.parseInt(request.getValue("size")));
			}
		}

		private void doOptionDebug() {

			if (request.getValue("type").equals("shutdown")) {
				logger.info("{}: Recieved Shutdown request", id);
				stop = true;
				try {
					serverSock.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				executor.shutdown();
			}

			else if (request.getValue("type").equals("sleep")) {
				if (request.getValue("sleep").equals("true")) {
					sleep = true;
					logger.debug("Get sleep request, sleep enabled");
				} else if (request.getValue("sleep").equals("false")) {
					sleep = false;
					logger.debug("Get sleep disable request, sleep disabled");
				}
				header = "HTTP/1.1 200 OK";
			}
		}

		/*
		 * Other methods
		 */

		/**
		 * Add tweets to database
		 */
		private void addTweet() throws ParseException, NullPointerException {

			JSONParser parser = new JSONParser();
			JSONObject json = new JSONObject();

			json = (JSONObject) parser.parse(postData);
			JSONArray tags = (JSONArray) json.get("hashtags");
			String tweet = json.get("tweet").toString();

			boolean fails = false;
			for (int tempId : serverList.getIdSet()) {
				if (tempId != id && !stop) {
					ServerConnection connection = new ServerConnection();
					logger.info("Sending post to secondary {}, version: {}",
							tempId, data.getDBVersion());
					try {
						connection.sendConnection("POST",
								serverList.getIP(tempId), "tweets?version="
										+ data.getDBVersion(), postData);
					} catch (IOException | BadResponseException
							| ParseException e) {
						fails = true;
						logger.error("Posting tweets to server {} fails: {}",
								tempId, e);
					}

					if (sleep) {
						try {
							Thread.sleep(5 * 1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}

			for (int i = 0; i < tags.size(); i++) {
				String tag = tags.get(i).toString();
				logger.info("#{}, Storing tweet: '{}' to tag: '{}'", id, tweet,
						tag);
				data.put(tag, tweet);
			}

			if (fails) {
				header = "HTTP/1.1 500 Internal Server Error";
				for (int tempID : serverList.getIdSet()) {
					if (tempID != id) {
						ServerConnection connection = new ServerConnection();
						try {
							connection.sendConnection(
									"DELETE",
									serverList.getIP(tempID),
									"?version=dbVersion&size="
											+ String.valueOf(tags.size())
											+ data.getDBVersion(), postData);
						} catch (IOException | BadResponseException
								| ParseException e) {
							fails = true;
							logger.error(
									"Delete request of server {} error: {}",
									tempID, e);
						}
					}
				}
				return;
			}
		}

		private void addTweetSecondary() throws ParseException,
				NullPointerException, VersionNotMatchException {

			JSONParser parser = new JSONParser();
			JSONObject json = new JSONObject();

			json = (JSONObject) parser.parse(postData);
			JSONArray tags = (JSONArray) json.get("hashtags");
			String tweet = json.get("tweet").toString();
			int pVersion = Integer.parseInt(request.getValue("version"));

			for (int i = 0; i < tags.size(); i++) {
				String tag = tags.get(i).toString();
				logger.info("#{}, Storing tweet: '{}' to tag: '{}'", id, tweet,
						tag);
				while (!data.put(tag, tweet, pVersion, i)) {
					// logger.info(
					// "Local version too small, wait for smaller patches. Local: {}, DB: {}",
					// data.getDBVersion(), pVersion + i);
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
					}
				}
			}
		}

		/**
		 * Notify all other servers that a new server has been added to the
		 * current system
		 * 
		 * @param targetIp
		 *            the server to send message
		 * @param newId
		 *            the ID of the new server.
		 * @param newIP
		 *            the IP of the new server.
		 */
		private void sendUpdateNotification(String targetIp, int newId,
				String newIP) throws IOException, BadResponseException,
				ParseException {

			JSONObject json = new JSONObject();

			json.put("id", newId);
			json.put("ip", newIP);

			String jStr = json.toString();

			// Create connection
			ServerConnection connection = new ServerConnection();
			connection.sendConnection("POST", targetIp, "server?type=update",
					jStr);
			if (connection.getHeader().equals("HTTP/1.1 200 OK")) {
				logger.info("Successfully updated server: {}", targetIp);
			} else {
				throw new BadResponseException();
			}
		}

		public void run() {
			try {

				try {
					parseRequest();
				} catch (InvalidHttpRequestException e) {
					logger.info("Got Invalid request from, sending 400 to FE");
					header = "HTTP/1.1 400 Bad Request";
					finish(header, null);
					return;
				}

				switch (request.getMethod()) {
				case "POST":
					// if endpoint is not tweets
					if (request.getUriPath().equals("/tweets")) {
						doPostTweet();
					} else if (request.getUriPath().equals("/server")) {
						doPostServer();
					} else if (request.getUriPath().equals("/update")) {
						doPostUpdate();
					} else {
						logger.info("Unknown endpoint, return 400 to FE");
						header = "HTTP/1.1 400 Bad Request";
					}
					break;
				case "GET":
					// if endpoint is not tweets
					if (request.getUriPath().equals("/tweets")) {
						doGetTweet();
					} else if (request.getUriPath().equals("/server")) {
						doGetServer();
					} else if (request.getUriPath().equals("/data")) {
						doGetData();
					} else if (request.getUriPath().equals("/update")) {
						doGetUpdate();
					} else {
						logger.info("Unknown endpoint, return 400 to FE");
						header = "HTTP/1.1 400 Bad Request";
					}
					break;
				case "DELETE":
					if (request.getUriPath().equals("/tweets"))
						doDeleteTweet();
					break;

				case "OPTIONS":
					if (request.getUriPath().equals("/debug")) {
						doOptionDebug();
					} else {
						logger.info("Unknown endpoint, return 400 to FE");
						header = "HTTP/1.1 400 Bad Request";
					}
				default:
					header = "HTTP/1.1 400 Bad Request";
					break;
				}
				finish(header, body);
			} catch (Exception e) {
				logger.info("Handling request {} error: {}",
						request.toString(), e);
				e.printStackTrace();
			}
		}
	}

	public static void main(String args[]) {
		DataServer ds = null;
		if (args.length == 2) {
			ds = new DataServer(args[0], args[1]);
		} else if (args.length == 4) {
			ds = new DataServer(args[0], args[1], args[2], args[3]);
		}
		ds.start();
	}
}