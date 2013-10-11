require "java"

Dir["./vendor/**/*.jar"].each { |j|
  $CLASSPATH << j
}

class ConsumerTest
  include java.lang.Runnable

  def initialize(stream, thread_num)
    @stream = stream
    @thread_num = thread_num
  end

  def run
    puts "running = #{@thread_num}"
    iterator = @stream.iterator
    @stream.each do |item|
      p @thread_num
      p String.from_java_bytes(item.message)
    end
  rescue
    p $!
  end
end

class ConsumerGroupExample
  def initialize(zookeeper, group_id, topic, num_threads)
    @consumer = kafka.consumer.Consumer.create_java_consumer_connector(
      create_consumer_config(zookeeper, group_id))
    @executor = java.util.concurrent.Executors.new_fixed_thread_pool(num_threads)
    @num_threads = num_threads
    @topic = topic
  end

  def run
    streams_map = @consumer.create_message_streams(
      { @topic => @num_threads.to_java(:int) })
    streams = streams_map[@topic]
    streams.each_with_index do |stream, i|
      @executor.submit(ConsumerTest.new(stream, i))
    end
  end

  def shutdown
    @consumer.shutdown
    @executor.shutdown
  end

  private

  def create_consumer_config(zookeeper, group_id)
    props = java.util.Properties.new
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", group_id)
    props.put("zookeeper.session.timeout.ms", "1000")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    kafka.consumer.ConsumerConfig.new(props)
  end

  def kafka
    Java::Kafka
  end
end

example = ConsumerGroupExample.new(
  ARGV[0] || raise("missing arg!"),
  ARGV[1] || raise("missing arg!"),
  ARGV[2] || raise("missing arg!"),
  3
)
example.run
sleep(60)
puts "shutdown"
example.shutdown
