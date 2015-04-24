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

import bes.injector.Injector;
import bes.injector.InjectionExecutor;

public class InjectorPlus extends Injector
{
    public InjectorPlus(String poolName)
    {
        super(poolName);
    }

    public static class InjectionExecutorPlus extends InjectionExecutor implements ExecutorPlus
    {
        protected InjectionExecutorPlus(int maxWorkers, int maxTasksQueued)
        {
            super(maxWorkers, maxTasksQueued);
        }
    }

    public InjectionExecutorPlus newExecutor(int maxWorkers, int maxTasksQueued)
    {
        return addExecutor(new InjectionExecutorPlus(maxWorkers, maxTasksQueued));
    }
}
