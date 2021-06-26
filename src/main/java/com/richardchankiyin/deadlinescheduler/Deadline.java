package com.richardchankiyin.deadlinescheduler;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

public class Deadline {
	private long requestid;
	private long deadlineepochtime;
	public Deadline(long requestid, long deadlineepochtime) {
		this.requestid = requestid;
		this.deadlineepochtime = deadlineepochtime;
	}
	public long getRequestid() {
		return requestid;
	}
	public long getDeadlineepochtime() {
		return deadlineepochtime;
	}
	public String toString() {
		return String.format("[requestid=%s|deadlineepochtime=%s]", this.requestid, this.deadlineepochtime);
	}
}

class DeadlineFactory {
	private AtomicLong count;
	public DeadlineFactory() {
		count = new AtomicLong(0);
	}
	public Deadline createDeadline(long deadlineepochtime) {
		return new Deadline(count.getAndAdd(1), deadlineepochtime);
	}
}

class DeadlineComparator implements Comparator<Deadline>{

	@Override
	public int compare(Deadline o1, Deadline o2) {
		if (o1.getDeadlineepochtime() < o2.getDeadlineepochtime()) {
			return -1;
		} else {
			if (o1.getDeadlineepochtime() > o2.getDeadlineepochtime()) {
				return 1;
			}
			return 0;
		}
	}
	
}
