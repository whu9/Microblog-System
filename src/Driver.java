import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.parser.ParseException;

public class Driver {

	DataServer ds = null;

	String ip = "192.168.1.150:10021";

	public void run() throws InterruptedException {
		ExecutorService executor = Executors.newFixedThreadPool(10);

		executor.execute(new DriverThread(1));
		Thread.sleep(500);

		executor.execute(new DriverThread(4));
		Thread.sleep(500);

		executor.execute(new DriverThread(5));
		Thread.sleep(500);

		executor.execute(new DriverThread(2));
		Thread.sleep(500);

		executor.execute(new DriverThread(3));
		Thread.sleep(500);

		executor.execute(new DriverThread(8));
		Thread.sleep(500);
		
		executor.execute(new DriverThread(6));
		Thread.sleep(2000);
		
		executor.execute(new DriverThread(7));
		Thread.sleep(10*1000);
		
		executor.execute(new DriverThread(9));
		Thread.sleep(500);
		
		executor.execute(new DriverThread(10));
		Thread.sleep(500);
		
		executor.execute(new DriverThread(11));
		Thread.sleep(500);
		
		executor.execute(new DriverThread(12));
		Thread.sleep(500);
		
		executor.execute(new DriverThread(13));
		Thread.sleep(500);
	}

	class DriverThread implements Runnable {

		int index;
		ServerConnection con;
		String body;

		public DriverThread(int index) {
			this.index = index;
		}

		@Override
		public void run() {
			switch (index) {
			case 1:
				ds = new DataServer("10021", "1");
				ds.start();
				try {
					Thread.sleep(10 * 1000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				ds.stop();
				break;

			case 2:
				FrontEndServer fs = new FrontEndServer(10011, ip);
				fs.start();
				break;
			case 3:
				FrontEndServer fs2 = new FrontEndServer(10012, ip);
				fs2.start();
				break;

			case 4:
				DataServer ds2 = new DataServer("10022", "2", "1", ip);
				ds2.start();
				break;

			case 5:
				DataServer ds3 = new DataServer("10023", "3", "1", ip);
				ds3.start();
				break;

			case 6:
				con = new ServerConnection();
				String body = "{\"text\": \"#hello i am #tweet\"}";
				try {
					con.sendConnection("POST", "localhost:10011", "tweets",
							body);
				} catch (IOException | BadResponseException | ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;

			case 7:
				con = new ServerConnection();
				try {
					con.sendConnection("OPTIONS", "localhost:10021", "debug?type=shutdown",
							"");
				} catch (IOException | BadResponseException | ParseException e) {
				}
				break;
				
			case 8:
				con = new ServerConnection();
				try {
					con.sendConnection("OPTIONS", "localhost:10021", "debug?type=sleep&sleep=true",
							"");
				} catch (IOException | BadResponseException | ParseException e) {
				}
				break;
				
			case 9:
				con = new ServerConnection();
				body = "{\"text\": \"#hello i am a #tweet\"}";
				try {
					con.sendConnection("POST", "localhost:10012", "tweets",
							body);
				} catch (IOException | BadResponseException | ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
				
			case 10:
				con = new ServerConnection();
				body = "{\"text\": \"#CS682 testing pushing a #tweet\"}";
				try {
					con.sendConnection("POST", "localhost:10011", "tweets",
							body);
				} catch (IOException | BadResponseException | ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
				
			case 11:
				con = new ServerConnection();
				body = "{\"text\": \"#Giants won the #worldseries\"}";
				try {
					con.sendConnection("POST", "localhost:10011", "tweets",
							body);
				} catch (IOException | BadResponseException | ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
				
			case 12:
				con = new ServerConnection();
				try {
					con.sendConnection("GET", "localhost:10011", "tweets?q=tweet",
							"");
				} catch (IOException | BadResponseException | ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
				
			case 13:
				con = new ServerConnection();
				body = "{\"text\": \"#Giants won the #worldseries\"}";
				try {
					con.sendConnection("GET", "localhost:10011", "tweets?q=tweet",
							"");
				} catch (IOException | BadResponseException | ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;

			default:
				break;
			}

			System.out.println("**********************************");
			System.out.println(con.getHeader());
			System.out.println(con.getBody());
			System.out.println("**********************************");
		}
	}

	public static void main(String[] args) {
		Driver driver = new Driver();
		try {
			driver.run();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
