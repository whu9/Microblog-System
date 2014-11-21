import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * @author Yi Wang A data structure that stores information about servers in the
 *         system
 *
 */
public class FrontEndList {
	HashSet<String> list;
	Lock lock;

	public FrontEndList() {
		list = new HashSet<String>();
		lock = new Lock();
	}
	
	public void addServer(String ip){
		lock.lockWrite();
		list.add(ip);
		lock.unlockWrite();
	}

	/**
	 * delete a failed server from the list
	 * 
	 * @param id
	 *            the id of the failed server
	 */
	public void deleteServer(String ip) {
		lock.lockWrite();
		list.remove(ip);
		lock.unlockWrite();
	}

	public HashSet<String> getList() {
		lock.lockRead();
		HashSet<String> IdSet = list;
		lock.unlockRead();
		return IdSet;
	}

	public JSONObject getJson() {
		JSONObject obj = new JSONObject();
		JSONArray arr = new JSONArray();
		for (String ip : list) {
			arr.add(ip);
		}
		obj.put("fe", arr);
		return obj;

	}

	public boolean contains(String ip) {
		lock.lockRead();
		boolean result = list.contains(ip);
		lock.unlockRead();
		return result;
	}

	public int size() {
		lock.lockRead();
		int size = this.list.size();
		lock.unlockRead();
		return size;
	}
}
