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
package bes.injector.microbench;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;

public class BlockingForkJoinPool extends ForkJoinPool implements ExecutorPlus
{
    final Semaphore permits;
    public BlockingForkJoinPool(int threads, int maxTasksQueued)
    {
        super(threads);
        permits = new Semaphore(maxTasksQueued);
    }

    public void execute(final Runnable task)
    {
        permits.acquireUninterruptibly();
        super.execute(new Runnable()
        {
            public void run()
            {
                try
                {
                    task.run();
                }
                finally
                {
                    permits.release();
                }
            }
        });
    }

    public void maybeExecuteInline(Runnable runnable)
    {
        execute(runnable);
    }
}
