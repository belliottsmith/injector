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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BlockingThreadPoolExecutor extends ThreadPoolExecutor implements ExecutorPlus
{

    public BlockingThreadPoolExecutor(int threads, int maxTasksQueued)
    {
        this(threads, new LinkedBlockingQueue<Runnable>(maxTasksQueued));
    }

    public BlockingThreadPoolExecutor(int threads, BlockingQueue<Runnable> queue)
    {
        super(threads, threads, 60L, TimeUnit.SECONDS, queue,
              new ThreadFactory(){
                  public Thread newThread(Runnable r)
                  {
                      Thread thread = new Thread(r);
                      thread.setDaemon(true);
                      return thread;
                  }
              },
              new RejectedExecutionHandler()
              {
                  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
                  {
                      try
                      {
                          executor.getQueue().offer(r, 1L, TimeUnit.MILLISECONDS);
                      }
                      catch (InterruptedException e)
                      {
                      }
                  }
              });
    }

    public void maybeExecuteInline(Runnable runnable)
    {
        execute(runnable);
    }
}
