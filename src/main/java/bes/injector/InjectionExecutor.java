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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import bes.concurrent.WaitQueue;

public class InjectionExecutor extends AbstractExecutorService
{

    /**
     * Called directly before the task is executed.
     * This method should ensure no exceptions can be thrown during its execution.
     */
    protected void beforeExecute(Runnable task) { }

    /**
     * Called after the task has been executed, successfully or not.
     * This method should ensure no exceptions can be thrown during its execution.
     *
     * @param task the task that was executed
     * @param failure null if success; otherwise the exception that caused the task to fail
     */
    protected void afterExecute(Runnable task, Throwable failure) { }

    // non-final so this class can be extended by users, whilst not exposing this internal detail
    Injector injector;

    private final int maxWorkers;
    private final int maxTasksQueued;

    // stores both a set of work permits and task permits:
    //   bottom 48 bits are number of queued tasks, in the range [0..maxTasksQueued]   (initially 0)
    //   top 16 bits are number of work permits available in the range [0..maxWorkers]   (initially maxWorkers)
    private final AtomicLong permits = new AtomicLong();

    final Queue<Runnable> tasks;
    final Work asWork = new Work(this);

    // producers wait on this when there is no room on the queue
    private final WaitQueue hasRoom = new WaitQueue();
    private final AtomicLong totalBlocked = new AtomicLong();
    private final AtomicInteger currentlyBlocked = new AtomicInteger();

    private final CountDownLatch terminated = new CountDownLatch(1);
    private volatile boolean shutdown = false;

    protected InjectionExecutor(int maxWorkers, int maxTasksQueued)
    {
        this(maxWorkers, maxTasksQueued, new ConcurrentLinkedQueue<Runnable>());
    }
    protected InjectionExecutor(int maxWorkers, int maxTasksQueued, Queue<Runnable> tasks)
    {
        if (maxWorkers >= 1 << 16)
            throw new IllegalArgumentException("Unsupported thread count: max is 65535");
        this.maxWorkers = maxWorkers;
        this.maxTasksQueued = maxTasksQueued;
        this.permits.set(maxWorkers * WORK_PERMIT);
        this.tasks = tasks;
    }

    // schedules another worker for this injector if there is work outstanding and there are no spinning threads that
    // will self-assign to it in the immediate future
    boolean maybeSchedule(boolean internal)
    {
        if (injector.spinningCount.get() > 0 || !takeWorkPermit(true))
            return false;

        injector.schedule(asWork, internal);
        return true;
    }

    /**
     * Execute the provided Runnable as soon as a worker becomes available. The task may be executed
     * on any of the Injector's shared worker threads, however the caller will not typically schedule
     * the thread, only doing so if there is currently no active worker capable of promptly serving it
     * in the injector, or this executor's task queue is full.
     *
     * @param task the task to execute
     */
    public void execute(Runnable task)
    {
        if (task == null)
            throw new NullPointerException();

        // we check the shutdown status initially to ensure we never accept a task submitted after shutdown()
        // or shutdownNow() have exited. otherwise, if we are not terminated, a worker could grab it from the queue
        // before we repair after the fact
        if (shutdown)
            throw new RejectedExecutionException();

        // we add to the queue first, so that when a worker takes a task permit it can be certain there is a task available
        // this permits us to schedule threads non-spuriously; it also means work is serviced fairly
        tasks.add(task);

        /**
         * we recheck the shutdown status after adding to the queue, to ensure we haven't raced.
         * if we are shutdown, and we fail to remove ourselves, we were added out of order with another
         * task that successfully incremented the task permits before the shutdown flag was set _and_
         * we have already been dequeued by a worker; in this case we cannot reject the task, since it's
         * being (or has been) processed, but we also must honour the prior task's successful submission,
         * so we have to increment our permit count
         */
        if (shutdown && tasks.remove(task))
            throw new RejectedExecutionException();

        long update = this.permits.incrementAndGet();

        if (taskPermits(update) == 1)
        {
            // we only need to schedule a thread if there are no tasks already waiting to be processed, as the prior
            // enqueue  that moved the permit count from zero will have already started a spinning worker (if necessary),
            // and spinning workers multiply if they encounter work, and only spin down if there is no work available
            injector.maybeStartSpinningWorker(false);
        }
        // we consider any available work permits to count towards our queue limit, since the resource use is the same;
        // the work can be considered allocated to each of the available permits, and effectively 'not queued'
        else if (taskPermits(update) > maxTasksQueued + workPermits(update))
        {
            // register to receive a signal once a task is processed bringing the queue below its threshold
            WaitQueue.Signal s = hasRoom.register();

            // we will only be signalled once the queue drops below full, so this creates equivalent external behaviour
            // however the advantage is that we never wake-up spuriously
            long latest = permits.get();
            if (taskPermits(latest) > maxTasksQueued + workPermits(latest))
            {
                // if we're blocking, we might as well directly schedule a worker if we aren't already at max
                if (takeWorkPermit(true))
                    injector.schedule(asWork, false);
                totalBlocked.incrementAndGet();
                currentlyBlocked.incrementAndGet();
                s.awaitUninterruptibly();
                currentlyBlocked.decrementAndGet();
            }
            else // don't propagate our signal when we cancel, just cancel
                s.cancel();
        }
    }

    /**
     * takes permission to perform a task, if any are available; once taken it is guaranteed
     * that a proceeding call to tasks.poll() will return some work
     */
    boolean takeTaskPermit()
    {
        while (true)
        {
            long current = permits.get();
            long update = current - 1;

            if (taskPermits(current) == 0)
                return false;

            if (permits.compareAndSet(current, update))
            {
                if (taskPermits(update) == maxTasksQueued && hasRoom.hasWaiters())
                    hasRoom.signalAll();
                return true;
            }
        }
    }

    // takes a work permit and (optionally) a task permit simultaneously; if one of the two is unavailable, returns false
    boolean takeWorkPermit(boolean takeTaskPermit)
    {
        long delta = WORK_PERMIT + (takeTaskPermit ? 1 : 0);
        while (true)
        {
            long current = permits.get();
            if ((workPermits(current) == 0) | (taskPermits(current) == 0))
                return false;
            long update = current - delta;
            if (permits.compareAndSet(current, update))
            {
                if (takeTaskPermit && taskPermits(update) == maxTasksQueued && hasRoom.hasWaiters())
                    hasRoom.signalAll();
                return true;
            }
        }
    }

    boolean hasTasks()
    {
        return taskPermits(permits.get()) > 0;
    }

    // gives up a work permit
    void returnWorkPermit()
    {
        while (true)
        {
            long current = permits.get();
            long update = current + WORK_PERMIT;
            if (permits.compareAndSet(current, update))
                return;
        }
    }

    // only to be called on encountering an *unexpected* exception (i.e. bug, bad VM state, OOM, etc.)
    void returnTaskPermit()
    {
        while (true)
        {
            long current = permits.get();
            long update = current + 1;
            if (permits.compareAndSet(current, update))
                return;
        }
    }

    /**
     * The calling thread, if there is a work permit available, temporarily becomes a worker
     * in this pool and executes the task inline. If there are no work permits available,
     * this call behaves like a normal invocation of execute(), placing the task on the queue
     * and letting one of the pool's workers handle it when available.
     *
     * @param task the task to execute
     */
    public void maybeExecuteInline(Runnable task)
    {
        if (!takeWorkPermit(false))
        {
            execute(task);
        }
        else
        {
            try
            {
                executeInternal(task);
            }
            finally
            {
                returnWorkPermit();
                maybeSchedule(false);
            }
        }
    }

    void executeInternal(Runnable task)
    {
        Throwable failure = null;
        try
        {
            beforeExecute(task);
            task.run();
        }
        catch (Throwable t)
        {
            failure = t;
        }
        afterExecute(task, failure);
    }

    /**
     * Stops further tasks from being submitted to the executor.
     * This is only guaranteed to stop submissions that were initiated after this call exits;
     * any execute() call entered even fractionally before this has a chance of being submitted
     * for processing after we exit.
     */
    public synchronized void shutdown()
    {
        shutdown = true;
        maybeTerminate();
    }

    /**
     * Stops further tasks from being submitted to the executor.
     * This is only guaranteed to stop submissions that were initiated after this call exits;
     * any execute() call entered even fractionally before this has a chance of being submitted
     * for processing after we exit.
     *
     * @return the tasks that were prevented from executing
     */
    public synchronized List<Runnable> shutdownNow()
    {
        shutdown = true;
        List<Runnable> aborted = new ArrayList<>();
        // we busy-spin trying to take a permit if the queue is non-empty, to reduce the window
        // in which an execution may be submitted after this call exits successfully
        while (!tasks.isEmpty())
            while (takeTaskPermit())
                aborted.add(tasks.poll());
        maybeTerminate();
        return aborted;
    }

    // once shutdown = true, this is called after any state change that might
    // result in the executor having terminated and, if so, signals termination
    void maybeTerminate()
    {
        while (true)
        {
            // we read tasks.isEmpty() before reading our permit state;
            // the alternative ordering permits a permit to be added
            // and a task to be dequeued, and for us not to notice
            boolean safeToTerminate = tasks.isEmpty();
            long permits = this.permits.get();

            // once we see a state with an empty task queue, we know we are _safe_ to terminate
            // because all execute() calls that insert to an empty queue after the shutdown flag
            // is set are guaranteed to be able to remove themselves without their task executing

            // the only question is: _are_ we terminated? this answers that
            if (taskPermits(permits) > 0 || workPermits(permits) < maxWorkers)
                return;

            if (safeToTerminate)
            {
                injector.executors.remove(this);
                terminated.countDown();
                return;
            }

            // however if we appear to be terminated, but don't have an empty task queue,
            // the termination state will resolve shortly but right now cannot be determined.
            // so we suspend a brief period to let any threads that raced on execute() to repair
            LockSupport.parkNanos(10000);
        }
    }

    public boolean isShutdown()
    {
        return shutdown;
    }

    public boolean isTerminated()
    {
        return terminated.getCount() == 0;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        terminated.await(timeout, unit);
        return isTerminated();
    }

    public long getPendingTasks()
    {
        return Math.max(0, taskPermits(permits.get()));
    }

    public int getActiveTasks()
    {
        return maxWorkers - (int) workPermits(permits.get());
    }

    public long getAllTimeBlockedProducers()
    {
        return totalBlocked.get();
    }

    public int getCurrentlyBlockedProducers()
    {
        return currentlyBlocked.get();
    }

    public int getMaxThreads()
    {
        return maxWorkers;
    }

    private static long WORK_PERMIT = 1L << 48;
    private static long TASK_BITMASK = -1L >>> 16;

    private static long taskPermits(long permits)
    {
        return permits & TASK_BITMASK;
    }

    private static long workPermits(long permits)
    {
        return permits >>> 48;
    }
}
