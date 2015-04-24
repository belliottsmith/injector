package bes.injector.microbench;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.IgnoreExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkProcessor;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class DisruptorExecutor extends AbstractExecutorService implements ExecutorPlus
{

    final RingBuffer<RContainer> ringBuffer;
    final WorkProcessor<RContainer>[] workProcessors;
    final ExecutorService workExec;

    public DisruptorExecutor(int threadCount, int bufferSize, WaitStrategy waitStrategy)
    {
        ringBuffer = RingBuffer.createMultiProducer(new EventFactory<RContainer>()
        {

            @Override
            public RContainer newInstance()
            {
                return new RContainer();
            }
        }, bufferSize, waitStrategy);
        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
        Sequence workSequence = new Sequence(-1);
        workProcessors = new WorkProcessor[threadCount];
        for (int i = 0 ; i < threadCount ; i++)
        {
            workProcessors[i] = new WorkProcessor<RContainer>(ringBuffer, sequenceBarrier,
                handler, new IgnoreExceptionHandler(), workSequence);
        }
        workExec = Executors.newFixedThreadPool(workProcessors.length, new ThreadFactory()
        {
            public Thread newThread(Runnable r)
            {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }
        });
        for (WorkProcessor p : workProcessors)
            workExec.execute(p);
    }

    @Override
    public void shutdown()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isShutdown()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isTerminated()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void execute(Runnable command)
    {
        long sequence = ringBuffer.next();
        RContainer event = ringBuffer.get(sequence);
        event.runnable = command;
        ringBuffer.publish(sequence);
    }

    public void maybeExecuteInline(Runnable runnable)
    {
        execute(runnable);
    }

    private static final class RContainer
    {
        volatile Runnable runnable;
    }

    static final WorkHandler<RContainer> handler = new WorkHandler<RContainer>()
    {
        @Override
        public void onEvent(RContainer event) throws Exception
        {
            Runnable r = event.runnable;
            if (r != null)
                r.run();
        }
    };
}
