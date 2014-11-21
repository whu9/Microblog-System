import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.parser.ParseException;

public class Test {

	private static final int tweet1 = 1;
	private static final int tweet2 = 2;
	private static final int tweet3 = 3;
	private static final int tweet4 = 4;
	private static final int tweet5 = 5;
	private static final int tweet6 = 6;
	private static final int tweet7 = 7;
	private static final int tweet8 = 8;
	private static final int tweet9 = 9;
	private static final int get1 = 10;
	private static final int get2 = 11;
	private static final int get3 = 12;
	private static final int get4 = 13;
	private static final int get5 = 14;
	private static final int get6 = 15;
	private static final int stop1 = 16;
	private static final int stop2 = 17;
	private static final int sleepEnable = 18;
	private static final int sleepDisable = 19;

	DataServer ds = null;
	int caseNum = 0;

	public Test(int caseNum) {
		this.caseNum = caseNum;
	}

	public void run() throws InterruptedException {
		ExecutorService executor = Executors.newFixedThreadPool(10);

		switch (caseNum) {
		case 1:
			executor.execute(new DriverThread(tweet1));
			Thread.sleep(500);
			executor.execute(new DriverThread(tweet2));
			Thread.sleep(500);
			executor.execute(new DriverThread(get1));
			Thread.sleep(500);
			executor.execute(new DriverThread(tweet3));
			Thread.sleep(500);
			executor.execute(new DriverThread(get2));
			Thread.sleep(500);
			break;
		case 2:
			executor.execute(new DriverThread(tweet4));
			Thread.sleep(500);
			executor.execute(new DriverThread(tweet5));
			Thread.sleep(500);
			executor.execute(new DriverThread(get3));
			Thread.sleep(500);
			executor.execute(new DriverThread(tweet6));
			Thread.sleep(500);
			executor.execute(new DriverThread(get4));
			Thread.sleep(500);
			break;
		case 3:
			executor.execute(new DriverThread(stop1));
			break;
		case 4:
			executor.execute(new DriverThread(tweet7));
			Thread.sleep(500);
			executor.execute(new DriverThread(tweet8));
			Thread.sleep(500);
			executor.execute(new DriverThread(get5));
			Thread.sleep(500);
			break;
		case 5:
			executor.execute(new DriverThread(sleepEnable));
			Thread.sleep(500);
			executor.execute(new DriverThread(tweet9));
			Thread.sleep(2 * 1000);
			executor.execute(new DriverThread(stop2));
			Thread.sleep(10 * 1000);
			executor.execute(new DriverThread(get6));
			Thread.sleep(500);
		default:
			break;
		}

		executor.shutdown();
	}

	class DriverThread implements Runnable {

		int index;
		ServerConnection con;
		String body;

		public DriverThread(int index) {
			this.index = index;
		}

		private void send(String method, String ip, String para, String body) {

			con = new ServerConnection();
			try {
				con.sendConnection(method, ip, para, body);
			} catch (IOException | BadResponseException | ParseException e) {
			}
		}

		@Override
		public void run() {

			System.out.println("**********************************");
			switch (index) {
			case tweet1:
				System.out.println("Send request: POST /tweet to 10.0.1.10");
				body = "{\"text\": \"#case1 #hello i am a #tweet\"}";
				System.out.println("Send Request body: " + body + "\n");
				send("POST", "10.0.1.10:10100", "tweets", body);
				break;

			case tweet2:
				System.out.println("Send request: POST /tweet to 10.0.1.11");
				body = "{\"text\": \"#case1 i am a #anothertweet\"}";
				System.out.println("Send Request body: " + body + "\n");
				send("POST", "10.0.1.11:10100", "tweets", body);
				break;

			case tweet3:
				System.out.println("Send request: POST /tweet to 10.0.1.10");
				body = "{\"text\": \"case1 should #work\"}";
				System.out.println("Send Request body: " + body + "\n");
				send("POST", "10.0.1.10:10100", "tweets", body);
				break;

			case get1:
				System.out
						.println("Send request: GET /tweet?q=case1 to 10.0.1.10");
				send("GET", "10.0.1.10:10100", "tweets?q=case1", "");
				break;

			case get2:
				System.out
						.println("Send request: GET /tweet?q=tweet to 10.0.1.11");
				send("GET", "10.0.1.11:10100", "tweets?q=tweet", "");
				break;

			// for case2: new nodes

			case tweet4:
				System.out.println("Send request: POST /tweet to 10.0.1.10");
				body = "{\"text\": \"#case2 #hello i am a #tweet\"}";
				System.out.println("Send Request body: " + body + "\n");
				send("POST", "10.0.1.10:10100", "tweets", body);
				break;

			case tweet5:
				System.out.println("Send request: POST /tweet to 10.0.1.11");
				body = "{\"text\": \"#case2 #giant won the #worldseries\"}";
				System.out.println("Send Request body: " + body + "\n");
				send("POST", "10.0.1.11:10100", "tweets", body);
				break;

			case tweet6:
				System.out.println("Send request: POST /tweet to 10.0.1.12");
				body = "{\"text\": \"case2 should #work\"}";
				System.out.println("Send Request body: " + body + "\n");
				send("POST", "10.0.1.12:10100", "tweets", body);
				break;

			case get3:
				System.out
						.println("Send request: GET /tweet?q=case1 to 10.0.1.10");
				send("GET", "10.0.1.12:10100", "tweets?q=case1", "");
				break;

			case get4:
				System.out
						.println("Send request: GET /tweet?q=giant to 10.0.1.12");
				send("GET", "10.0.1.12:10100", "tweets?q=case2", "");
				break;

			// for case 3: killing primary

			case stop1:
				System.out
						.println("Send request: OPTIONS /debug?type=shutdown to 10.0.1.1");
				send("OPTIONS", "10.0.1.1:10100", "debug?type=shutdown", "");
				break;

			// for case 4: more posts

			case tweet7:
				System.out.println("Send request: POST /tweet to 10.0.1.10");
				body = "{\"text\": \"#case3 is new #primary work fine\"}";
				System.out.println("Send Request body: " + body + "\n");
				send("POST", "10.0.1.10:10100", "tweets", body);
				break;

			case tweet8:
				System.out.println("Send request: POST /tweet to 10.0.1.10");
				body = "{\"text\": \"#case3 hello world\"}";
				System.out.println("Send Request body: " + body + "\n");
				send("POST", "10.0.1.10:10100", "tweets", body);
				break;

			case get5:
				System.out
						.println("Send request: GET /tweet?q=case3 to 10.0.1.10");
				send("GET", "10.0.1.10:10100", "tweets?q=case3", "");
				break;
				
				//for case 5:

			case sleepEnable:
				System.out
						.println("Send request: OPTIONS debug?type=sleep&sleep=true to 10.0.1.2");
				send("OPTIONS", "10.0.1.2:10100", "debug?type=sleep&sleep=true", "");
				break;
				
			case tweet9:
				System.out.println("Send request: POST /tweet to 10.0.1.10");
				body = "{\"text\": \"#case4 Testing Version #Mismatch\"}";
				System.out.println("Send Request body: " + body + "\n");
				send("POST", "10.0.1.10:10100", "tweets", body);
				break;
				
			case stop2:
				System.out
						.println("Send request: OPTIONS /debug?type=shutdown to 10.0.1.2");
				send("OPTIONS", "10.0.1.2:10100", "debug?type=shutdown", "");
				break;
				
			case get6:
				System.out
						.println("Send request: GET /tweet?q=case4 to 10.0.1.10");
				send("GET", "10.0.1.10:10100", "tweets?q=case4", "");
				break;

			// case 7:
			// con = new ServerConnection();
			// String body = "{\"text\": \"#case1 #hello i am a #tweet\"}";
			// try {
			// con.sendConnection("POST", "10.0.1.11:10100", "tweets",
			// body);
			// } catch (IOException | BadResponseException | ParseException e) {
			// }
			// break;
			//
			// case 8:
			// con = new ServerConnection();
			// try {
			// con.sendConnection("OPTIONS", "localhost:10021",
			// "debug?type=sleep&sleep=true", "");
			// } catch (IOException | BadResponseException | ParseException e) {
			// }
			// break;
			//
			// case 9:
			// con = new ServerConnection();
			// body = "{\"text\": \"#hello i am a #tweet\"}";
			// try {
			// con.sendConnection("POST", "localhost:10012", "tweets",
			// body);
			// } catch (IOException | BadResponseException | ParseException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			// break;
			//
			// case 10:
			// con = new ServerConnection();
			// body = "{\"text\": \"#CS682 testing pushing a #tweet\"}";
			// try {
			// con.sendConnection("POST", "localhost:10011", "tweets",
			// body);
			// } catch (IOException | BadResponseException | ParseException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			// break;
			//
			// case 11:
			// con = new ServerConnection();
			// body = "{\"text\": \"#Giants won the #worldseries\"}";
			// try {
			// con.sendConnection("POST", "localhost:10011", "tweets",
			// body);
			// } catch (IOException | BadResponseException | ParseException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			// break;
			//
			// case 12:
			// con = new ServerConnection();
			// try {
			// con.sendConnection("GET", "localhost:10011",
			// "tweets?q=tweet", "");
			// } catch (IOException | BadResponseException | ParseException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			// break;
			//
			// case 13:
			// con = new ServerConnection();
			// body = "{\"text\": \"#Giants won the #worldseries\"}";
			// try {
			// con.sendConnection("GET", "localhost:10011",
			// "tweets?q=tweet", "");
			// } catch (IOException | BadResponseException | ParseException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			// break;

			default:
				break;
			}

			System.out.println(con.getHeader());
			System.out.println(con.getBody());
		}
	}

	public static void main(String[] args) {
		Test driver = new Test(Integer.parseInt(args[0]));
		try {
			driver.run();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
