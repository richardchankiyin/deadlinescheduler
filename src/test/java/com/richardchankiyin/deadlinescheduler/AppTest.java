package com.richardchankiyin.deadlinescheduler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
	private final static Logger logger = LoggerFactory.getLogger(AppTest.class);
	private final static long WAITTIMEMS = 200;
    /**
     * Test
     */
    @Test
    public void testDeadlineEngineSchedule()
    {
        DeadlineEngine engine = new DeadlineEngineImpl();
        DeadlineEngineImpl engineImpl = (DeadlineEngineImpl)engine;
        long deadlineMs1 = 100;
        long requestId1 = engine.schedule(deadlineMs1);
        assertTrue(requestId1 == 0L);
        assertTrue(deadlineMs1 == engineImpl.enquire(requestId1));
        assert(deadlineMs1 == engineImpl.nextDeadline());
        assertTrue(1 == engineImpl.queueSize());
        assertTrue(1 == engine.size());
        
        long deadlineMs2 = 100;
        long requestId2 = engine.schedule(deadlineMs2);
        assertTrue(requestId2 == 1L);
        assertTrue(deadlineMs2 == engineImpl.enquire(requestId2));
        assert(deadlineMs1 == engineImpl.nextDeadline());
        assertTrue(2 == engineImpl.queueSize());
        assertTrue(2 == engine.size());
        
        long deadlineMs3 = 1000;
        long requestId3 = engine.schedule(deadlineMs3);
        assertTrue(requestId3 == 2L);
        assertTrue(deadlineMs3 == engineImpl.enquire(requestId3));
        assert(deadlineMs1 == engineImpl.nextDeadline());
        assertTrue(3 == engineImpl.queueSize());
        assertTrue(3 == engine.size());
    }
    
    @Test
    public void testDeadlineEngineCancel() {
    	DeadlineEngine engine = new DeadlineEngineImpl();
        long deadlineMs1 = 100;
        long requestId1 = engine.schedule(deadlineMs1);
        long deadlineMs2 = 100;
        long requestId2 = engine.schedule(deadlineMs2);
        assertTrue(2 == engine.size());
        assertTrue(engine.cancel(requestId1));
        assertTrue(1 == engine.size());
        assertTrue(engine.cancel(requestId2));
        assertTrue(0 == engine.size());
        assertFalse(engine.cancel(100));
    }
    
    @Test
    public void testPollNoRequestId() {
    	DeadlineEngine engine = new DeadlineEngineImpl();
    	DeadlineEngineImpl engineImpl = (DeadlineEngineImpl)engine;
        long deadlineMs1 = 100;
        long requestId1 = engine.schedule(deadlineMs1);
        long deadlineMs2 = 100;
        long requestId2 = engine.schedule(deadlineMs2);
        assertTrue(deadlineMs1 == engineImpl.enquire(requestId1));
        assertTrue(deadlineMs2 == engineImpl.enquire(requestId2));
        assertTrue(0==engine.poll(100, s->{}, 0));
        assertTrue(0==engine.poll(100, s->{}, -1));
        assertTrue(0==engine.poll(50, s->{}, 10));
        assertTrue(2 == engine.size());
    }
    
    @Test
    public void testPollLessThanMax() throws Exception{
    	DeadlineEngine engine = new DeadlineEngineImpl();
    	DeadlineEngineImpl engineImpl = (DeadlineEngineImpl)engine;
    	Arrays.asList(100L, 200L, 300L, 400L, 500L).forEach(s->engine.schedule(s));
    	assertTrue(5 == engine.size());
    	assertTrue(5 == engineImpl.queueSize());
    	long nowMs = 250L;
    	Set<Long> requestIdsHandled = new HashSet<>();
    	assertTrue(2 == engine.poll(nowMs, s->{ logger.debug("handling {}", s);requestIdsHandled.add(s); }, 5));
    	assertTrue(3 == engine.size());
    	assertTrue(3 == engineImpl.queueSize());
    	Thread.sleep(WAITTIMEMS);
    	assertTrue(2 == requestIdsHandled.size());
    	assertTrue(requestIdsHandled.contains(0L));
    	assertTrue(requestIdsHandled.contains(1L));
    }
    
    @Test
    public void testPollMaxItemHit() throws Exception {
    	DeadlineEngine engine = new DeadlineEngineImpl();
    	DeadlineEngineImpl engineImpl = (DeadlineEngineImpl)engine;
    	Arrays.asList(100L, 200L, 300L, 400L, 500L).forEach(s->engine.schedule(s));
    	assertTrue(5 == engine.size());
    	assertTrue(5 == engineImpl.queueSize());
    	long nowMs = 450L;
    	Set<Long> requestIdsHandled = new HashSet<>();
    	assertTrue(3 == engine.poll(nowMs, s->{ logger.debug("handling {}", s);requestIdsHandled.add(s); }, 3));
    	assertTrue(2 == engine.size());
    	assertTrue(2 == engineImpl.queueSize());
    	Thread.sleep(WAITTIMEMS);
    	assertTrue(3 == requestIdsHandled.size());
    	assertTrue(requestIdsHandled.contains(0L));
    	assertTrue(requestIdsHandled.contains(1L));
    	assertTrue(requestIdsHandled.contains(2L));
    	assertTrue(400L == engineImpl.nextDeadline());
    	long nowMs2 = 451L;
    	assertTrue(1 == engine.poll(nowMs2, s->{ logger.debug("handling {}", s);requestIdsHandled.add(s); }, 3));
    	Thread.sleep(WAITTIMEMS);
    	assertTrue(4 == requestIdsHandled.size());
    	assertTrue(requestIdsHandled.contains(3L));
    }
    
    @Test
    public void testPollFromNonOrderScheduleTime() throws Exception{
    	DeadlineEngine engine = new DeadlineEngineImpl();
    	DeadlineEngineImpl engineImpl = (DeadlineEngineImpl)engine;
    	Arrays.asList(100L, 200L, 500L, 400L, 300L).forEach(s->engine.schedule(s));
    	assertTrue(5 == engine.size());
    	assertTrue(5 == engineImpl.queueSize());
    	long nowMs = 450L;
    	Set<Long> requestIdsHandled = new HashSet<>();
    	assertTrue(4 == engine.poll(nowMs, s->{ logger.debug("handling {}", s);requestIdsHandled.add(s); }, 5));
    	assertTrue(1 == engine.size());
    	assertTrue(1 == engineImpl.queueSize());
    	Thread.sleep(WAITTIMEMS);
    	assertTrue(4 == requestIdsHandled.size());
    	assertTrue(requestIdsHandled.contains(0L));
    	assertTrue(requestIdsHandled.contains(1L));
    	assertTrue(requestIdsHandled.contains(3L));
    	assertTrue(requestIdsHandled.contains(4L));
    }
    
    @Test
    public void testPollWithCancelledItem() throws Exception {
    	DeadlineEngine engine = new DeadlineEngineImpl();
    	DeadlineEngineImpl engineImpl = (DeadlineEngineImpl)engine;
    	Arrays.asList(100L, 200L, 300L, 400L, 500L).forEach(s->engine.schedule(s));
    	assertTrue(5 == engine.size());
    	assertTrue(5 == engineImpl.queueSize());
    	long nowMs = 450L;
    	Set<Long> requestIdsHandled = new HashSet<>();
    	assertTrue(engine.cancel(1L));
    	assertTrue(engine.cancel(3L));
    	assertTrue(3 == engine.size());
    	assertTrue(5 == engineImpl.queueSize());
    	assertTrue(2 == engine.poll(nowMs, s->{ logger.debug("handling {}", s);requestIdsHandled.add(s); }, 5));
    	Thread.sleep(WAITTIMEMS);
    	assertTrue(2 == requestIdsHandled.size());
    	assertTrue(requestIdsHandled.contains(0L));
    	assertTrue(requestIdsHandled.contains(2L));
    	assertTrue(1 == engine.size());
    	assertTrue(1 == engineImpl.queueSize());
    }
}
