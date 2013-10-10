require "java"

Dir["./vendor/**/*.jar"].each { |j|
  $CLASSPATH << j
}

class ConsumerGroupExample
  def initialize(zookeeper, group_id, topic)
    @consumer = kafka.consumer.Consumer.create_java_consumer_connector(
      create_consumer_config(zookeeper, group_id))
    @topic = topic
  end

  def run
    streams_map = @consumer.createMessageStreams({ @topic => 1.to_java(:int) })
    streams = streams_map[@topic]
    streams.first.each do |item|
      p String.from_java_bytes(item.message)
    end
  end

  def shutdown
    @consumer.shutdown
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
  ARGV[2] || raise("missing arg!")
)
example.run
sleep(1000)
puts "shutdown"
example.shutdown
