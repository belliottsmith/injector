package bes.injector;/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

final class Worker extends AtomicReference<Work> implements Runnable
{
    private static final long TARGET_SLEEP_NANOS = TimeUnit.MICROSECONDS.toNanos(10L);
    private static final long MAX_SLEEP_NANOS = TimeUnit.MILLISECONDS.toNanos(1L);

    final Long workerId;
    final Thread thread;
    final Injector injector;

    // prevStopCheck stores the value of injector.stopCheck after we last incremented it; if it hasn't changed,
    // we know nobody else was spinning in the interval, so we increment our soleSpinnerSpinTime accordingly,
    // and otherwise we set it to zero; this is then used to terminate the final spinning thread, as the coordinated
    // strategy can only work when there are multiple threads spinning (as more sleep time must elapse than real time)
    long prevStopCheck = 0;
    long soleSpinnerSpinTime = 0;

    Worker(Long workerId, Work initialState, Injector injector, ThreadFactory threadFactory)
    {
        this.injector = injector;
        this.workerId = workerId;
        thread = threadFactory.newThread(this);
        set(initialState);
        thread.start();
    }

    public void run()
    {
        while (true)
        {
            /**
             * we maintain two important invariants:
             * 1)   after exiting spinning phase, we ensure at least one more task on _each_ queue will be processed
             *      promptly after we begin, assuming any are outstanding on any pools. this is to permit producers to
             *      avoid signalling if there are _any_ spinning threads. we achieve this by simply calling
             *      maybeSchedule() on each queue if, on decrementing the spin counter, we hit zero.
             * 2)   before processing a task on a given queue, we attempt to assign another worker to the _same queue
             *      only_; this allows a producer to skip signalling work if there are task permits already available,
             *      and in conjunction with invariant (1) ensures that if any thread was spinning when a task was added
             *      to any executor, that task will be processed immediately if work permits are available
             */

            InjectionExecutor assigned = null;
            Runnable task = null;
            try
            {
                while (true)
                {
                    if (isSpinning() && !selfAssign())
                    {
                        doWaitSpin();
                        continue;
                    }

                    // if stop was signalled, go to sleep (don't try self-assign; being put to sleep is rare and means
                    // we're over capacity, so let's obey it whenever we receive it. we don't apply this constraint to
                    // producers, who may reschedule us before we go to sleep)
                    if (stop())
                        while (isStopped())
                            LockSupport.park();

                    // we can be assigned any state from STOPPED, so loop if we don't actually have any tasks assigned
                    assigned = get().assigned;
                    if (assigned == null)
                        continue;

                    // if we've been assigned, we have a task permit already taken for us, so get our task
                    task = assigned.tasks.poll();

                    // once assigned nobody will change our state, so we can simply set it to WORKING
                    // (which is also a state that will never be interrupted externally)
                    set(Work.WORKING);

                    while (true)
                    {
                        assigned.maybeSchedule(true);
                        Runnable run = task;
                        task = null;
                        assigned.executeInternal(run);

                        if (!assigned.takeTaskPermit())
                            break;

                        task = assigned.tasks.poll();
                    }

                    // return our work permit, and maybe signal shutdown
                    assigned.returnWorkPermit();
                    if (assigned.isShutdown())
                    {
                        InjectionExecutor wasAssigned = assigned;
                        assigned = null;
                        wasAssigned.maybeTerminate();
                    }
                    assigned = null;

                    // try to immediately reassign ourselves some work; if we fail, start spinning
                    if (!selfAssign())
                        startSpinning();
                }
            }
            catch (Throwable t)
            {

                /**
                 * in general this should only happen in case of, e.g., OOM or some equivalent dangerous state,
                 * so there's only so much we can do to ensure correctness. For safety, we let this thread die if possible.
                 *
                 * Even then, generally it should not be possible for exceptions to be thrown anywhere between adopting
                 * a task and attempting to execute it (or beforeExecute()), unless the VM state is badly corrupted,
                 * so we are only really ensuring our wider injector state is correct, and that work cannot be assigned to us.
                 *
                 * In this event we don't care if a waiting task may not be served _promptly_ (i.e. may have to wait for
                 * another thread to complete its current work before serving the rest of the queue), only that it is served
                 * eventually, assuming those other threads aren't stuck. If we are the last worker, we try to continue
                 * where we left off, as otherwise tasks may be left unprocessed.
                 */

                Work state = getAndSet(Work.DEAD);
                if (assigned != null)
                {
                    assigned.returnWorkPermit();
                    if (task != null)
                    {
                        assigned.tasks.add(task);
                        state.assigned.returnTaskPermit();
                    }
                }
                else if (state.isAssigned())
                {
                    state.assigned.returnWorkPermit();
                    state.assigned.returnTaskPermit();
                }

                if (state.isSpinning())
                    injector.spinningCount.decrementAndGet();

                boolean terminate = true;
                if (injector.workerCount.decrementAndGet() == 0)
                {
                    // then we check if there's actually any work to do; if not, we terminate
                    boolean hasWork = false;
                    for (InjectionExecutor executor : injector.executors)
                        hasWork |= executor.hasTasks();

                    // finally we check to see no new threads have been started
                    if (hasWork && injector.workerCount.compareAndSet(0, 1))
                    {
                        set(Work.SPINNING);
                        injector.spinningCount.incrementAndGet();
                        terminate = false;
                    }
                }

                Thread thread = Thread.currentThread();
                Thread.UncaughtExceptionHandler handler = thread.getUncaughtExceptionHandler();
                if (handler != null)
                    handler.uncaughtException(thread, t);

                if (terminate)
                    break;
            }
        }
    }

    // try to assign this worker the provided work
    // valid states to assign are SPINNING, STOP_SIGNALLED, (ASSIGNED);
    // restores invariants of the various states (i.e. spinningCount, descheduled collection and thread park status)
    boolean assign(Work work, boolean self)
    {
        Work state = get();
        while (state.canAssign(self))
        {
            if (!compareAndSet(state, work))
            {
                state = get();
                continue;
            }
            // if we were spinning, exit the state (decrement the count); this is valid even if we are already spinning,
            // as the assigning thread will have incremented the spinningCount
            if (state.isSpinning())
                stopSpinning();

            // if we're being descheduled, place ourselves in the descheduled collection
            if (work.isStop())
                injector.descheduled.put(workerId, this);

            // if we're currently stopped, and the new state is not a stop signal
            // (which we can immediately convert to stopped), unpark the worker
            if (state.isStopped() && (!work.isStop() || !stop()))
                LockSupport.unpark(thread);
            return true;
        }
        return false;
    }

    // try to assign ourselves an executor with work available
    private boolean selfAssign()
    {
        // if we aren't permitted to assign in this state, fail
        if (!get().canAssign(true))
            return false;
        for (InjectionExecutor exec : injector.executors)
        {
            if (exec.takeWorkPermit(true))
            {
                Work work = exec.asWork;
                // we successfully started work on this executor, so we must either assign it to ourselves or ...
                if (assign(work, true))
                    return true;
                // ... if we fail, schedule it to another worker
                injector.schedule(work, true);
                // and return success as we must have already been assigned a task
                assert get().isAssigned();
                return true;
            }
        }
        return false;
    }

    // we can only call this when our state is WORKING, and no other thread may change our state in this case;
    // so in this case only we do not need to CAS. We increment the spinningCount and add ourselves to the spinning
    // collection at the same time
    private void startSpinning()
    {
        assert get() == Work.WORKING;
        injector.spinningCount.incrementAndGet();
        set(Work.SPINNING);
    }

    // exit the spinning state; if there are no remaining spinners, we immediately try and schedule work for all executors
    // so that any producer is safe to not spin up a worker when they see a spinning thread (invariant (1) above)
    private void stopSpinning()
    {
        if (injector.spinningCount.decrementAndGet() == 0)
            for (InjectionExecutor executor : injector.executors)
                executor.maybeSchedule(true);
        prevStopCheck = soleSpinnerSpinTime = 0;
    }

    // perform a sleep-spin, incrementing injector.stopCheck accordingly
    private void doWaitSpin()
    {
        // pick a random sleep interval based on the number of threads spinning, so that
        // we should always have a thread about to wake up, but most threads are sleeping
        long sleep, sleepMax;
        sleepMax = TARGET_SLEEP_NANOS * injector.spinningCount.get();
        sleepMax = Math.min(MAX_SLEEP_NANOS, sleepMax);
        if (TARGET_SLEEP_NANOS >= sleepMax)
            sleep = TARGET_SLEEP_NANOS;
        else
            sleep = ThreadLocalRandom.current().nextLong(TARGET_SLEEP_NANOS, sleepMax);

        long start = System.nanoTime();
        // place ourselves in the spinning collection; if we clash with another thread just exit
        Long targetWakeup = start + sleep;

        if (injector.spinning.putIfAbsent(targetWakeup, this) != null)
            return;

        LockSupport.parkNanos(sleep);

        // remove ourselves (if haven't been already) - we should be at or near the front, so should be cheap-ish
        injector.spinning.remove(targetWakeup, this);

        long end = System.nanoTime();
        long spin = end - start;
        long stopCheck = injector.stopCheck.addAndGet(spin);
        maybeStop(stopCheck, end);
        if (prevStopCheck + spin == stopCheck)
            soleSpinnerSpinTime += spin;
        else
            soleSpinnerSpinTime = 0;
        prevStopCheck = stopCheck;
    }

    private static final long stopCheckInterval = TimeUnit.MILLISECONDS.toNanos(10L);

    // stops a worker if elapsed real time is less than elapsed spin time, as this implies the equivalent of
    // at least one worker achieved nothing in the interval. we achieve this by maintaining a stopCheck which
    // is initialised to a negative offset from realtime; as we spin we add to this value, and if we ever exceed
    // realtime we have spun too much and deschedule; if we get too far behind realtime, we reset to our initial offset
    private void maybeStop(long stopCheck, long now)
    {
        long delta = now - stopCheck;
        if (delta <= 0)
        {
            // if stopCheck has caught up with present, we've been spinning too much, so if we can atomically
            // set it to the past again, we should stop a worker
            if (injector.stopCheck.compareAndSet(stopCheck, now - stopCheckInterval))
            {
                // try and stop ourselves;
                // if we've already been assigned work, stop another worker
                if (!assign(Work.STOP_SIGNALLED, true))
                    injector.schedule(Work.STOP_SIGNALLED, true);
            }
        }
        else if (soleSpinnerSpinTime > stopCheckInterval && injector.spinningCount.get() == 1)
        {
            // permit self-stopping
            assign(Work.STOP_SIGNALLED, true);
        }
        else
        {
            // if stop check has gotten too far behind present, update it so new spins can affect it
            while (delta > stopCheckInterval * 2 && !injector.stopCheck.compareAndSet(stopCheck, now - stopCheckInterval))
            {
                stopCheck = injector.stopCheck.get();
                delta = now - stopCheck;
            }
        }
    }

    private boolean isSpinning()
    {
        return get().isSpinning();
    }

    private boolean stop()
    {
        return get().isStop() && compareAndSet(Work.STOP_SIGNALLED, Work.STOPPED);
    }

    private boolean isStopped()
    {
        return get().isStopped();
    }
}
