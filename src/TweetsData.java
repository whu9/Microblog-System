import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;
import java.util.Stack;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class TweetsData {

	private HashMap<String, TagData> map;
	private HashMap<Integer, TweetLog> logMap;
	String name = "Unnamed";
	Logger logger;
	private Lock lock;

	private int dbVersion;

	/**
	 * default constructor
	 */
	public TweetsData() {
		map = new HashMap<String, TagData>();
		logMap = new HashMap<Integer, TweetLog>();
		logger = LogManager.getLogger("DataObjectLogger");
		lock = new Lock();
		dbVersion = 0;
	}

	/**
	 * construct that give Database a name
	 * 
	 * @param name
	 */
	public TweetsData(String name) {
		this();
		this.name = name;
		logger.info("Logger initialized: {}", name);
	}

	/**
	 * put a "tag/tweet" pair database
	 * 
	 * @param tag
	 *            the hashtag
	 * @param tweet
	 *            the tweet
	 */
	public int put(String tag, String tweet) {
		logger.info("{}: putting tweet: '{}' to tag: '{}'", name, tweet, tag);
		lock.lockWrite();
		// if may has the tag, add the tweet to it. other wise create a new
		// entry
		if (map.containsKey(tag)) {
			map.get(tag).add(tweet);
			logMap.put(dbVersion, new TweetLog(tag, tweet));

			logger.info("{}: tag: '{}' found, tweet '{}' added, version: {}\n",
					name, tag, tweet, map.get(tag).version);
		} else {
			map.put(tag, new TagData(tweet));
			logMap.put(dbVersion, new TweetLog(tag, tweet));
			logger.info(
					"{}: tag: '{}' not found, putting tweet: '{}'\n, current version: {},",
					name, tag, tweet, map.get(tag).version);
		}
//		logger.info(this.logMap.toString());
		int tempVersion = dbVersion;
		lock.unlockWrite();
		return tempVersion;
	}

	public boolean put(String tag, String tweet, int primaryVersion, int diff)
			throws VersionNotMatchException {

		logger.info("local: {}, Primary: {}", dbVersion, primaryVersion + diff);
		lock.lockRead();
		if (dbVersion < primaryVersion + diff) {
			lock.unlockRead();
			return false;
		}
		lock.unlockRead();

		lock.lockWrite();
		if (dbVersion > primaryVersion + diff) {
			logger.info("Current dbversion does not match primary version, abort");
			lock.unlockWrite();
			throw new VersionNotMatchException();
		} else if (dbVersion == primaryVersion + diff) {

			logger.info("{}: putting tweet: '{}' to tag: '{}'", name, tweet,
					tag);
			// if may has the tag, add the tweet to it. other wise create a new
			// entry
			if (map.containsKey(tag)) {
				map.get(tag).add(tweet);
				logMap.put(dbVersion, new TweetLog(tag, tweet));

				logger.info(
						"{}: tag: '{}' found, tweet '{}' added, version: {}\n",
						name, tag, tweet, map.get(tag).version);
			} else {
				map.put(tag, new TagData(tweet));
				logMap.put(dbVersion, new TweetLog(tag, tweet));
				logger.info(
						"{}: tag: '{}' not found, putting tweet: '{}'\n, current version: {}",
						name, tag, tweet, map.get(tag).version);
			}
		}
		lock.unlockWrite();
		return true;
	}

	public void delete(int version, int size) {
		for (int i = version; i > version - size; i--) {
			String tag = logMap.get(i).getTag();
			String tweet = logMap.get(i).getTweet();
			map.get(tag).delete(tweet);
			dbVersion--;
		}
	}
	
	public void deleteTag(String tag){
		map.remove(tag);
	}

	/**
	 * Check if database contains the key.
	 * 
	 * @param key
	 *            the key needs to be checked
	 * @return true if database has the key, false otherwise.
	 */
	public boolean contains(String key) {
		lock.lockRead();
		boolean contains = map.containsKey(key);
		lock.unlockRead();
		return contains;
	}

	/**
	 * Get the current version number of the hashtag entry
	 * 
	 * @param key
	 *            the hashtag
	 * @return the current version number of the hashtag entry
	 */
	public int getVersion(String key) {
		lock.lockRead();
		int version = map.get(key).version;
		lock.unlockRead();
		return version;
	}

	/**
	 * Set the verison number to the one from DB, only use for cache
	 * 
	 * @param key
	 *            the version number needed to be updated
	 * @param version
	 *            new version number
	 */
	public void setVersion(String key, int version) {
		logger.info("{}: tag: '{}' version set to {}\n", name, key, version);
		lock.lockWrite();
		map.get(key).version = version;
		lock.unlockWrite();
	}

	public int getDBVersion() {
		lock.lockRead();
		int version = dbVersion;
		lock.unlockRead();
		return version;
	}

	/**
	 * Create a json object for the hashtag entry for FE
	 * 
	 * @param key
	 *            the hashtag
	 * @return the json object
	 */
	public JSONObject getJson(String key) {
		JSONObject json = new JSONObject();
		json.put("q", key);
		lock.lockRead();
		if (map.containsKey(key)) {
			json.put("v", getVersion(key));
			JSONArray tweets = new JSONArray();
			for (String t : map.get(key).tweets) {
				tweets.add(t);
			}
			json.put("tweets", tweets);
		} else {
			json.put("v", "0");
			JSONArray tweets = new JSONArray();
			json.put("tweets", tweets);
		}
		lock.unlockRead();
		return json;
	}

	public JSONObject getDBJson() {
		JSONObject obj = new JSONObject();
		JSONArray arr = new JSONArray();
		for (String tag : map.keySet()) {
			arr.add(getJson(tag));
		}
		obj.put("version", dbVersion);
		obj.put("data", arr);
		return obj;
	}

	public JSONObject getLogJson(int version) {
		JSONObject obj = new JSONObject();
		lock.lockRead();
		TweetLog log = logMap.get(version);
		lock.unlockRead();
		obj.put("tag", log.getTag());
		obj.put("tweet", log.getTweet());
		return obj;
	}

	/**
	 * Create a json object for the hashtag entry for Client
	 * 
	 * @param key
	 *            the hashtag
	 * @return the json object
	 */
	public JSONObject getClientJson(String key) {
		JSONObject json = new JSONObject();
		json.put("q", key);
		lock.lockRead();
		if (map.containsKey(key)) {
			JSONArray tweets = new JSONArray();
			for (String t : map.get(key).tweets) {
				tweets.add(t);
			}
			json.put("tweets", tweets);
		} else {
			JSONArray tweets = new JSONArray();
			json.put("tweets", tweets);
		}
		lock.unlockRead();
		return json;
	}

	/**
	 * Data structure for hashtag entry
	 * 
	 * @author Peter
	 *
	 */
	class TagData {
		private int version;
		private ArrayList<String> tweets;

		public TagData(String tweet) {
			tweets = new ArrayList<String>();
			version = 1;
			dbVersion++;
			tweets.add(tweet);
		}

		public void add(String tweet) {
			tweets.add(tweet);
			dbVersion++;
			version++;
		}

		public void delete(String tweet) {
			tweets.remove(tweet);
			dbVersion--;
			version--;
		}
	}

	class TweetLog {
		private String tag;
		private String tweet;

		public TweetLog(String tag, String tweet) {
			this.tag = tag;
			this.tweet = tweet;
		}

		public String getTag() {
			return tag;
		}

		public String getTweet() {
			return tweet;
		}

		public String toString() {
			return getTag() + ":" + getTweet();
		}
	}

}
