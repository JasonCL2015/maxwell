package com.zendesk.maxwell.producer;


import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.metrics.Metrics;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.StoppableTaskState;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class RocketMQCallback implements SendCallback {

	public static final Logger LOGGER = LoggerFactory.getLogger(MaxwellRocketMQProducer.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private final String key;
	private final Timer timer;
	private final MaxwellContext context;

	private Counter succeededMessageCount;
	private Counter failedMessageCount;
	private Meter succeededMessageMeter;
	private Meter failedMessageMeter;

	public RocketMQCallback(AbstractAsyncProducer.CallbackCompleter cc, Position position, String key, String json,
						 Timer timer, Counter producedMessageCount, Counter failedMessageCount, Meter producedMessageMeter,
						 Meter failedMessageMeter, MaxwellContext context) {
		this.cc = cc;
		this.position = position;
		this.key = key;
		this.json = json;
		this.timer = timer;
		this.succeededMessageCount = producedMessageCount;
		this.failedMessageCount = failedMessageCount;
		this.succeededMessageMeter = producedMessageMeter;
		this.failedMessageMeter = failedMessageMeter;
		this.context = context;
	}

	@Override
	public void onSuccess(SendResult sendResult) {
		this.succeededMessageCount.inc();
		this.succeededMessageMeter.mark();

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("->  key:" + key);
			LOGGER.debug("   " + this.json);
			LOGGER.debug("   " + position);
			LOGGER.debug("");
		}
		cc.markCompleted();
		timer.update(cc.timeToSendMS(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void onException(Throwable throwable) {
		this.failedMessageCount.inc();
		this.failedMessageMeter.mark();

		LOGGER.error(throwable.getClass().getSimpleName() + " @ " + position + " -- " + key);
		LOGGER.error(throwable.getLocalizedMessage());
		if (!this.context.getConfig().ignoreProducerError) {
			this.context.terminate((Exception) throwable);
			return;
		}
	}
}


public class MaxwellRocketMQProducer extends AbstractProducer {

	private final ArrayBlockingQueue<RowMap> queue;

	private final MaxwellRocketMQProducerWorker worker;

	public MaxwellRocketMQProducer(MaxwellContext context, Properties rocketmqProperties) {
		super(context);
		this.queue = new ArrayBlockingQueue<>(100);
		this.worker = new MaxwellRocketMQProducerWorker(context, rocketmqProperties, this.queue);
		Thread thread = new Thread(this.worker, "maxwell-rocketmq-worker");
		thread.setDaemon(true);
		thread.start();
	}

	@Override
	public void push(RowMap r) throws Exception {
		this.queue.put(r);
	}
}


class MaxwellRocketMQProducerWorker extends AbstractAsyncProducer implements Runnable, StoppableTask {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellRocketMQProducer.class);

	private DefaultMQProducer rocketmq;
	private String topic;
	private final String ddlTopic;
	private final RowMap.KeyFormat keyFormat;
	private final Timer metricsTimer;
	private final ArrayBlockingQueue<RowMap> queue;
	private Thread thread;
	private StoppableTaskState taskState;

	public MaxwellRocketMQProducerWorker(MaxwellContext context, Properties rocketmqProperties, ArrayBlockingQueue<RowMap> queue) {
		super(context);

		this.topic = rocketmqProperties.getProperty("topic");
		if ( this.topic == null ) {
			this.topic = "maxwell";
		}

		this.rocketmq = new DefaultMQProducer(rocketmqProperties.getProperty("producerGroup"));
		this.rocketmq.setNamesrvAddr(rocketmqProperties.getProperty("nameServerAddress"));
		try {
			rocketmq.start();
		} catch (MQClientException e) {
			LOGGER.debug("rocketmq start fail");
		}
		this.ddlTopic =  rocketmqProperties.getProperty("ddlTopic");

		keyFormat = RowMap.KeyFormat.HASH;

		Metrics metrics = context.getMetrics();
		this.metricsTimer = metrics.getRegistry().timer(metrics.metricName("message", "publish", "time"));

		this.queue = queue;
		this.taskState = new StoppableTaskState("MaxwellRocketmqProducerWorker");
	}

	@Override
	public void run() {
		this.thread = Thread.currentThread();
		while ( true ) {
			try {
				RowMap row = queue.take();
				if (!taskState.isRunning()) {
					taskState.stopped();
					return;
				}
				this.push(row);
			} catch ( Exception e ) {
				taskState.stopped();
				context.terminate(e);
				return;
			}
		}
	}

	@Override
	public void sendAsync(RowMap r, AbstractAsyncProducer.CallbackCompleter cc) throws Exception {
		String key = r.pkToJson(keyFormat);
		String value = r.toJSON(outputConfig);
		Message message;

		// using table name as tag
		if (r instanceof DDLMap) {
			message = new Message(ddlTopic, r.getTable(), key, value.getBytes());
		} else {
			message = new Message(topic, r.getTable(), key, value.getBytes());
		}

		RocketMQCallback callback = new RocketMQCallback(cc, r.getPosition(), key, value, this.metricsTimer,
				this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter, this.context);

		rocketmq.send(message, callback);
	}

	@Override
	public void requestStop() {
		taskState.requestStop();
		rocketmq.shutdown();
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {
		taskState.awaitStop(thread, timeout);
	}

	@Override
	public StoppableTask getStoppableTask() {
		return this;
	}
}
