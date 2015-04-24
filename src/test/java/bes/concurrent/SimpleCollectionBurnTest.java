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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class SimpleCollectionBurnTest
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
            System.out.println(String.format("Testing up to %d items for %dm", maxSignalsPerThread * threadCount,
                                             TimeUnit.NANOSECONDS.toMinutes(duration / 7)));
            final SimpleCollection c = new SimpleCollection();
            final long until = System.nanoTime() + (duration / 7);
            final CountDownLatch latch = new CountDownLatch(threadCount);
            for (int t = 0 ; t < threadCount ; t++)
            {
                new Thread(new Runnable()
                {
                    public void run()
                    {
                        ThreadLocalRandom rand = ThreadLocalRandom.current();
                        List<SimpleCollection.Node> items = new ArrayList<>();
                        while (System.nanoTime() < until)
                        {
                            int targetSize = rand.nextInt(maxSignalsPerThread);
                            while (items.size() < targetSize)
                                items.add(c.add(new Object()));
                            while (items.size() > targetSize)
                                items.remove(rand.nextInt(items.size())).remove();
                            if (!present(c, items.iterator()))
                                System.err.println("fail");
                        }
                        latch.countDown();
                    }
                }).start();
            }
            latch.await();
        }
    }

    static boolean present(SimpleCollection<Object> c, Iterator<SimpleCollection.Node> check)
    {
        if (!check.hasNext())
            return true;
        Object cur = check.next().item;
        for (Object o : c)
        {
            if (o == cur)
            {
                if (!check.hasNext())
                    return true;
                cur = check.next().item;
            }
        }
        return false;
    }

    public static void main(String[] args) throws InterruptedException
    {
        new SimpleCollectionBurnTest().testRemovals(TimeUnit.HOURS.toNanos(2L));
    }

}
