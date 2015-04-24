/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package bes.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class WaitQueueBurnTest
{

    @Test
    public void testRemovals() throws InterruptedException
    {
        testRemovals(TimeUnit.MINUTES.toNanos(2L));
    }

    public void testRemovals(long duration) throws InterruptedException
    {
        for (int i = 0 ; i < 7 ; i++)
        {
            final int maxSignalsPerThread = 4 << i;
            final int threadCount = 8;
            System.out.println(String.format("Testing up to %d signals for %dm", maxSignalsPerThread * threadCount,
                                             TimeUnit.NANOSECONDS.toMinutes(duration / 7)));
            final WaitQueue q = new WaitQueue();
            final long until = System.nanoTime() + (duration / 7);
            final CountDownLatch latch = new CountDownLatch(threadCount);
            for (int t = 0 ; t < threadCount ; t++)
            {
                new Thread(new Runnable()
                {
                    public void run()
                    {
                        ThreadLocalRandom rand = ThreadLocalRandom.current();
                        List<WaitQueue.Signal> signals = new ArrayList<>();
                        while (System.nanoTime() < until)
                        {
                            int targetSize = rand.nextInt(maxSignalsPerThread);
                            while (signals.size() < targetSize)
                                signals.add(q.register());
                            while (signals.size() > targetSize)
                                signals.remove(rand.nextInt(signals.size())).cancel();
                            if (!q.present(signals.iterator()))
                                System.err.println("fail");
                            q.hasWaiters();
                        }
                        latch.countDown();
                    }
                }).start();
            }
            latch.await();
        }
    }

    public static void main(String[] args) throws InterruptedException
    {
        new WaitQueueBurnTest().testRemovals(TimeUnit.HOURS.toNanos(2L));
    }

}
