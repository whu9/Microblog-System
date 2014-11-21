import java.util.HashMap;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


/**
 * 
 * @author Yi Wang
 * A data structure that stores information about servers in the system
 *
 */
public class ServerList {
	HashMap<Integer, String> map;
	HashMap<Integer, Integer> failures;
	Lock lock;

	public ServerList() {
		map = new HashMap<Integer, String>();
		failures = new HashMap<Integer, Integer>();
		lock = new Lock();
	}

	public void addServer(int id, String ip) {
		lock.lockWrite();
		map.put(id, ip);
		lock.unlockWrite();
	}

	/**
	 * delete a failed server from the list
	 * @param id the id of the failed server
	 */
	public void deleteServer(int id) {
		lock.lockWrite();
		map.remove(id);
		lock.unlockWrite();
	}

	public Set<Integer> getIdSet() {
		lock.lockRead();
		Set<Integer> IdSet = map.keySet();
		lock.unlockRead();
		return IdSet;
	}

	public String getIP(int id) {
		lock.lockRead();
		String ip = map.get(id);
		lock.unlockRead();
		return ip;
	}

	public boolean contains(int id) {
		return map.containsKey(id);
	}

	public boolean addFailure(int id) {
		lock.lockWrite();
		boolean result = false;
		if (!failures.containsKey(id))
			failures.put(id, 1);
		else {
			if (failures.get(id) >= 0) {
				int value = failures.get(id);
				System.out.println(value);
				if (value > 0)
					// TODO
					result = true;
				else
					failures.put(id, failures.get(id) + 1);
			} else if (failures.get(id) == -1) {
				result = false;
			}
		}
		lock.unlockWrite();
		// TODO
		return result;
	}

	public boolean hasFailure(int id) {
		lock.lockRead();
		boolean result = false;
		result = failures.containsKey(id);
		lock.unlockRead();
		return result;
	}

	public void handlingFailure(int id) {
		lock.lockWrite();
		failures.put(id, -1);
		lock.unlockWrite();
	}

	public int size() {
		lock.lockRead();
		int size = this.map.size();
		lock.unlockRead();
		return size;
	}

	public JSONObject getIPJson(int id) {
		String ip = map.get(id);
		JSONObject json = new JSONObject();
		json.put(id, ip);
		return json;
	}

	public JSONObject getIdSetJson() {
		JSONObject json = new JSONObject();
		JSONArray arr = new JSONArray();
		for (int id : map.keySet()) {
			arr.add(id);
		}
		json.put("idSet", arr);
		return json;
	}

	public void update(JSONObject obj) {
		for (Object id : obj.keySet()) {
			if (!this.map.containsKey(Integer.parseInt(id.toString()))) {
				map.put(Integer.parseInt(id.toString()), obj.get(id.toString())
						.toString());
			}
		}
	}

	public JSONObject getListJson() {
		lock.lockRead();
		JSONObject json = new JSONObject();
		for (int id : this.map.keySet()) {
			json.put(id, map.get(id));
		}
		lock.unlockRead();
		return json;
	}
}
