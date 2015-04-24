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

/**
 * Represents, and communicates changes to, a worker's work state - there are three non-actively-working
 * states (STOP_SIGNALLED, STOPPED, AND SPINNING) and two working states: WORKING, and (ASSIGNED), the last
 * being represented by a non-static instance with its "assigned" executor set.
 *
 * STOPPED:         indicates the worker is descheduled, and whilst accepts work in this state (causing it to
 *                  be rescheduled) it will generally not be considered for work until all other worker threads are busy.
 *                  In this state we should be present in the injector.descheduled collection, and should be parked
 * valid transitions -> (ASSIGNED)|SPINNING
 *
 * STOP_SIGNALLED:  the worker has been asked to deschedule itself, but has not yet done so; only entered from a SPINNING
 *                  state, and generally communicated by and to itself, but maybe set from any worker. this state may be
 *                  preempted and replaced with (ASSIGNED) or SPINNING
 *                  In this state we should be present in the injector.descheduled collection
 * valid transitions -> (ASSIGNED)|STOPPED|SPINNING
 *
 * SPINNING:        indicates the worker has no work to perform, so is performing friendly wait-based-spinning
 *                  until it either is (ASSIGNED) some work (by itself or another thread), or sent STOP_SIGNALLED
 *                  In this state we _may_ be in the injector.spinning collection (but only if we are in the middle of a sleep)
 * valid transitions -> (ASSIGNED)|STOP_SIGNALLED|SPINNING
 *
 * (ASSIGNED):      asks the worker to perform some work against the specified executor, and preassigns a task permit
 *                  from that executor so that in this state there is always work to perform.
 *                  In general a worker assigns itself this state, but sometimes it may assign another worker the state
 *                  either if there is work outstanding and no-spinning threads, or there is a race to self-assign
 * valid transition -> WORKING
 *
 * WORKING:         indicates the worker is actively processing an executor's task queue; in this state it accepts
 *                  no state changes/communications, except from itself; it usually exits this mode into SPINNING,
 *                  but if work is immediately available on another executor it self-triggers (ASSIGNED)
 * valid transitions -> SPINNING|(ASSIGNED)
 */

final class Work
{
    static final Work STOP_SIGNALLED = new Work();
    static final Work STOPPED = new Work();
    static final Work SPINNING = new Work();
    static final Work WORKING = new Work();
    static final Work DEAD = new Work();

    final InjectionExecutor assigned;

    Work(InjectionExecutor executor)
    {
        this.assigned = executor;
    }

    private Work()
    {
        this.assigned = null;
    }

    boolean canAssign(boolean self)
    {
        // we can assign work if there isn't new work already assigned (or we're dead) and either
        // 1) we are assigning to ourselves
        // 2) the worker we are assigning to is not already in the middle of WORKING
        return !(isDead() | isAssigned()) && (self || !isWorking());
    }

    boolean isSpinning()
    {
        return this == Work.SPINNING;
    }

    boolean isWorking()
    {
        return this == Work.WORKING;
    }

    boolean isStop()
    {
        return this == Work.STOP_SIGNALLED;
    }

    boolean isStopped()
    {
        return this == Work.STOPPED;
    }

    boolean isDead()
    {
        return this == Work.DEAD;
    }

    boolean isAssigned()
    {
        return assigned != null;
    }
}
