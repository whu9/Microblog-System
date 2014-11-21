public class Lock {

	private int writer;
	private int reader;

	/**
	 * Default constructor. Set writer and reader to 0.
	 */
	public Lock() {
		reader = 0;
		writer = 0;
	}

	/**
	 * If reader and writer are greater than 0, wait. Otherwise increment writer
	 * by 1
	 */
	public synchronized void lockWrite() {

		while (writer > 0 || reader > 0) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				System.out.println("Unlock write error: " + e);
			}
		}

		writer++;
	}

	/**
	 * Unlock write, decrement writer by 1
	 */
	public synchronized void unlockWrite() {
		if (writer > 0) {
			writer--;
			this.notifyAll();
		}
	}

	/**
	 * If writer is greater than 0, wait. Otherwise increment reader by 1
	 */
	public synchronized void lockRead() {

		while (writer != 0) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				System.out.println("Lock read error: " + e);
			}
		}
		reader++;

	}

	/**
	 * Decrement reader by 1. If the reader hits zero, notifyall.
	 */
	public synchronized void unlockRead() {
		reader--;

		if (reader == 0) {
			this.notifyAll();
		}
	}
}