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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class ExecutorBenchmark
{

    private ExecutorPlus[] exec;
    private Semaphore workGate;
    private int opWorkTokens;
    private long opSleepNanos;
    private float opSleepChance;

    @Param({"128"})
    private int threads;

    @Param({"1:1"})
    // (in):(queued) both as multiples of thread count
    private String tasks;

    @Param({"INJECTOR"})
    private String type;

    @Param({"0.1"})
    private double opWork;

    @Param({"-1"})
    private double opWorkRatio;

    @Param({"0/0"})
    // (chance of sleep)/(length of sleep (us))
    private String opSleep;

    @Param({"1"})
    private int executorChainLength;

    @Setup
    public void setup()
    {
        if (opWorkRatio < 0)
            throw new IllegalStateException();

        String[] opSleepArgs = opSleep.split("/");
        opSleepChance = Float.parseFloat(opSleepArgs[0]);
        opSleepNanos = Long.parseLong(opSleepArgs[1]) * 1000L;
        opWorkTokens = (int) Math.ceil(opWork * opWorkRatio * (1d / executorChainLength));

        String[] taskArgs = tasks.split(":");
        int concurrentRequests = (int) (threads * Double.parseDouble(taskArgs[0]));
        int maxTasksQueued = (int) (threads * Double.parseDouble(taskArgs[1]));
        final InjectorPlus injector = new InjectorPlus("");
        exec = new ExecutorPlus[executorChainLength];
        workGate = new Semaphore(concurrentRequests, false);
        for (int i = 0 ; i < exec.length ; i++)
        {
            switch (ExecutorType.valueOf(type))
            {
                case INJECTOR:
                    exec[i] = injector.newExecutor(threads, maxTasksQueued);
                    break;
                case JDK:
                    exec[i] = new BlockingThreadPoolExecutor(threads, maxTasksQueued);
                    break;
                case FJP:
                    exec[i] = new BlockingForkJoinPool(threads, maxTasksQueued);
                    break;
                case DISRUPTOR_SPIN:
                    exec[i] = new DisruptorExecutor(threads, maxTasksQueued, new BusySpinWaitStrategy());
                    break;
                case DISRUPTOR_BLOCK:
                    exec[i] = new DisruptorExecutor(threads, maxTasksQueued, new BlockingWaitStrategy());
                    break;
            }
        }
    }

    private static double calcWorkRatio(long minMeasurementIntervalNanos)
    {
        int tokens = 0;
        long start = System.nanoTime();
        long end;
        do
        {
            tokens += 10000;
            Blackhole.consumeCPU(10000);
        } while ((end = System.nanoTime()) - start < minMeasurementIntervalNanos);
        return tokens / (((double)(end - start)) / 1000);
    }

    @Benchmark
    public void test()
    {
        workGate.acquireUninterruptibly();
        exec[0].execute(new Work(1));
    }

    private final class Work implements Runnable
    {
        final int executorIndex;

        private Work(int executorIndex)
        {
            this.executorIndex = executorIndex;
        }

        public void run()
        {
            Blackhole.consumeCPU(opWorkTokens);
            if (executorIndex < exec.length)
            {
                exec[executorIndex].maybeExecuteInline(new Work(executorIndex + 1));
            }
            else
            {
                if (opSleepNanos > 0 && ThreadLocalRandom.current().nextFloat() < opSleepChance)
                    LockSupport.parkNanos(opSleepNanos);
                workGate.release();
            }
        }
    }

    public static void main(String[] args) throws RunnerException, InterruptedException
    {
        boolean addPerf = false;
        Map<String, Integer> jmhParams = new HashMap<String, Integer>();
        jmhParams.put("forks", 1);
        jmhParams.put("producerThreads", 1);
        jmhParams.put("warmups", 5);
        jmhParams.put("warmupLength", 1);
        jmhParams.put("measurements", 5);
        jmhParams.put("measurementLength", 2);
        Map<String, String[]> benchParams = new LinkedHashMap<String, String[]>();
        benchParams.put("opSleep", new String[] { "0/0" });
        benchParams.put("opWork", new String[] { "1", "10", "100" });
        benchParams.put("tasks", new String[] { "0.5:1", "1:1", "4:1", "4:4" });
        benchParams.put("type", new String[] { "INJECTOR", "JDK" });
        benchParams.put("threads", new String[] { "32", "128", "512" });
        benchParams.put("executorChainLength", new String[] { "1" });
        for (String arg : args)
        {
            if (arg.equals("-perf"))
            {
                addPerf = true;
                continue;
            }
            String[] split = arg.split("=");
            if (split.length != 2)
                throw new IllegalArgumentException(arg + " malformed");
            if (jmhParams.containsKey(split[0]))
                jmhParams.put(split[0], Integer.parseInt(split[1]));
            else if (benchParams.containsKey(split[0]))
                benchParams.put(split[0], split[1].split(","));
            else
                throw new IllegalArgumentException(arg + " unknown property");
        }

        double workRatio = calcWorkRatio(TimeUnit.SECONDS.toNanos(1));

        ChainedOptionsBuilder builder = new OptionsBuilder()
            .include(".*ExecutorBenchmark.*")
            .forks(jmhParams.get("forks"))
            .threads(jmhParams.get("producerThreads"))
            .warmupIterations(jmhParams.get("warmups"))
            .warmupTime(TimeValue.seconds(jmhParams.get("warmupLength")))
            .measurementIterations(jmhParams.get("measurements"))
            .measurementTime(TimeValue.seconds(jmhParams.get("measurementLength")))
            .jvmArgs("-dsa", "-da")
            .param("opWorkRatio", String.format("%.3f", workRatio));

        if (addPerf)
            builder.addProfiler(org.openjdk.jmh.profile.LinuxPerfProfiler.class);

        System.out.println("Running with:");
        System.out.println(jmhParams);
        for (Map.Entry<String, String[]> e : benchParams.entrySet())
        {
            System.out.println(e.getKey() + ": " + Arrays.toString(e.getValue()));
            builder.param(e.getKey(), e.getValue());
        }
        new Runner(builder.build()).run();
    }
}
