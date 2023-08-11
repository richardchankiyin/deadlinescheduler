package com.richardchankiyin.deadlinescheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.Consumer;

public class DeadlineEngineImpl implements DeadlineEngine {
	private static final int WAITTIMEMS = 100;
	private Map<Long, Deadline> requestIdToDeadlineMap = null;
	private Queue<Deadline> deadlineQueue = null;
	private DeadlineFactory factory = null;
	public DeadlineEngineImpl() {
		factory = new DeadlineFactory();
		requestIdToDeadlineMap = new HashMap<>();
		deadlineQueue = new PriorityQueue<Deadline>(new DeadlineComparator());
	}

	@Override
	public long schedule(long deadlineMs) {
		Deadline deadline = factory.createDeadline(deadlineMs);
		long requestId = deadline.getRequestid();
		requestIdToDeadlineMap.put(requestId, deadline);
		if (!deadlineQueue.offer(deadline)) {
			requestIdToDeadlineMap.remove(requestId);
			throw new IllegalStateException("failed to schedule deadline: " + deadlineMs);
		}
		return requestId;
	}

	@Override
	public boolean cancel(long requestId) {
		Deadline deadline = requestIdToDeadlineMap.remove(requestId);
		return deadline != null;
	}

	private void pause() {
		try {
			Thread.sleep(WAITTIMEMS);
		} catch (Exception e) {
			
		}		
	}
	
	@Override
	public int poll(long nowMs, Consumer<Long> handler, int maxPoll) {
		boolean isContinue = true;
		int result = 0;
		if (maxPoll <= 0)
			isContinue = false;
		while (isContinue) {
			Deadline deadline = deadlineQueue.peek();
			// deadline time is found before current time, handle
			if (deadline != null && nowMs >= deadline.getDeadlineepochtime()) {
				// check whether this deadline is found in the map or not
				// if not that means its got cancelled and we will only
				// poll without increasing result and handle it
				long requestId = deadline.getRequestid();
				boolean isValid = requestIdToDeadlineMap.containsKey(requestId);
				deadlineQueue.poll();
				if (isValid) {
					this.handleDeadline(handler, deadline);
					requestIdToDeadlineMap.remove(requestId);
					++result;
				}
				
				// ideally result should be only <= maxPoll
				// however we do not want to enter infinite
				// loop due to bug therefore we make this
				// fail safe handling
				if (result >= maxPoll) {
					isContinue = false;
				}
			} else {
				// deadline time is after now, finish
				isContinue = false;
			}
			
			if (isContinue) {
				pause();
			}
		}
		return result;
	}
	
	private void handleDeadline(final Consumer<Long> handler, final Deadline deadline) {
		Thread t = new Thread(()->handler.accept(deadline.getRequestid()));
		t.setName(deadline.toString());
		t.start();
	}

	@Override
	public int size() {
		return requestIdToDeadlineMap.size();
	}
	
	public long enquire(long requestId) {
		Deadline deadline = requestIdToDeadlineMap.get(requestId);
		if (deadline == null) {
			throw new NullPointerException("no deadline timestamp found for request id: " + requestId);
		}
		return deadline.getDeadlineepochtime();
	}

	public int queueSize() {
		return deadlineQueue.size();
	}
	
	public long nextDeadline() {
		return deadlineQueue.peek().getDeadlineepochtime();
	}
}
