#!/usr/bin/env ruby
require 'eventmachine'

DOWNSTREAM_NUM = 3
BASE_DS_PORT = 9100
SR_DATA_PORT = 9000
SR_HEALTH_PORT = 9001
SR_CONFIG_FILE = "/tmp/statsd-router.conf"
SR_EXE_FILE = "../statsd-router"
SR_DS_HEALTH_CHECK_INTERVAL = 3.0
SR_DS_FLUSH_INTERVAL = 2.0
MONKEY_ACTION_INTERVAL_MIN = 0.1
MONKEY_ACTION_INTERVAL_MAX = 2.0
SR_HEALTH_CHECK_REQUEST = "ok"
SR_DS_PING_INTERVAL = 10.0
SR_PING_PREFIX = "statsd-cluster-test"

HEALTH_CHECK_PROBABILITY = 0.05
DS_TOGGLE_PROBABILITY = 0.1

SUCCESS_EXIT_STATUS = 0
FAILURE_EXIT_STATUS = 1
DEFAULT_TEST_TIMEOUT = 20

class DataServer < EventMachine::Connection
    def initialize(statsd_mock)
        @statsd_mock = statsd_mock
        @test_controller = statsd_mock.test_controller
    end

    def receive_data(data)
        now = Time.now.to_f
        data.split("\n").each do |d|
            next if d =~ /^#{SR_PING_PREFIX}/
            m = StatsdRouterTest.get_message_queue().select {|x| x[:data] == d}.first
            if m == nil
                @test_controller.abort("Failed to find \"#{d}\" in message queue")
            end
            m[:hashring].each do |h|
                ds = @statsd_mock.get_all[h]
                if ds.num == @statsd_mock.num
                    if !ds.healthy && (now - ds.last_stop_time > SR_DS_HEALTH_CHECK_INTERVAL * 2)
                        @test_controller.abort("#{@statsd_mock.num} got data though it is down")
                    end
                    StatsdRouterTest.get_message_queue().delete(m)
                    break
                else
                    if ds.healthy
                        if m[:timestamp] > ds.last_start_time && m[:timestamp] - ds.last_start_time < SR_DS_HEALTH_CHECK_INTERVAL * 2
                            next
                        end
                    else
                        next
                    end
                end
                @test_controller.abort("Hashring problem: #{m[:hashring]}, #{h} health status: #{ds.healthy}")
            end
            @test_controller.notify({source: "statsd", text: d})
        end
    end
end

class HealthServer < EventMachine::Connection
    def initialize(connections, statsd_mock)
        @statsd_mock = statsd_mock
        @connections = connections
        @connections << self
    end

    def receive_data(data)
        send_data("health: up\n")
        @statsd_mock.last_health_check_time = Time.now.to_f
    end

    def unbind
        @connections.delete(self)
    end
end

class StatsdMock
    attr_accessor :last_health_check_time, :message_queue, :num, :last_start_time, :last_stop_time
    attr_reader :test_controller
    @@all = []

    def initialize(data_port, health_port, num, test_controller)
        @@all << self
        @num = num
        @data_port = data_port
        @health_port = health_port
        @connections = []
        @last_health_check_time = Time.now.to_f
        @last_start_time = Time.now.to_f
        @last_stop_time = Time.now.to_f
        @test_controller = test_controller
        EventMachine::open_datagram_socket('0.0.0.0', @data_port, DataServer, self)
    end

    def healthy
        @health_server != nil
    end

    def start
        return if @health_server != nil
        now = Time.now.to_f
        @last_start_time = now
        @last_health_check_time = now
        @health_server = EventMachine::start_server('0.0.0.0', @health_port, HealthServer, @connections, self)
    end

    def stop
        return if @health_server == nil
        @last_stop_time = Time.now.to_f
        EventMachine::stop_server(@health_server)
        @health_server = nil
        @connections.each do |conn|
            conn.close_connection
        end
    end

    def get_all
        @@all
    end
end

class OutputHandler < EM::Connection
    def initialize(test_controller)
        @test_controller = test_controller
    end

    def receive_data(data)
        data.split("\n").each do |d|
            @test_controller.notify({source: "statsd-router", text: d})
        end
    end
end

module HealthClient
    def receive_data(data)
        if data != SR_HEALTH_CHECK_REQUEST
            puts "!!! Health check failed: expected \"#{SR_HEALTH_CHECK_REQUEST}\", got \"#{data}\""
        else
            puts "*** health check ok"
        end
    end
end

class StatsdRouterTest
    @@message_queue = []

    def health_check()
        if @health_connection == nil || @health_connection.error?
            if @health_connection != nil && @health_connection.error?
                puts "!!! Error while connecting to health port, restarting connection"
            end
            EventMachine.connect('127.0.0.1', SR_HEALTH_PORT, HealthClient) do |conn|
                @health_connection = conn
            end
        else
            @health_connection.send_data(SR_HEALTH_CHECK_REQUEST)
        end
    end

    def hashring(name)
        hash = 0
        name.each_byte {|b| hash = (hash * 31 + b) & 0xffffffffffffffff}
        a = (0...DOWNSTREAM_NUM).to_a
        a.reverse.each do |i|
            j = hash % (i + 1)
            k = a[j]
            if j != i
                a[j] = a[i]
                a[i] = k
            end
            hash = (hash * 7 + 5) / 3
        end
        a.reverse
    end

    def valid_metric(length)
        name = "statsd-cluster.count"
        number = rand(100).to_s
        name_length = name.length + number.length
        if name_length < length
            name += ("X" * (length - name_length) + number)
        end
        a = hashring(name)
        @counter = (@counter + 1) % 1000
        data = name + ":#{@counter}|c"
        {
            hashring: a,
            data: data,
            event: {source: "statsd", text: data}
        }
    end

    def invalid_metric(length)
        data = (0...length).map { (65 + rand(26)).chr }.join
        {
            data: data,
            event: {source: "statsd-router", text: "ERROR process_data_line: invalid metric #{data}"}
        }
    end

    def send_data_impl(*args)
        event_list = []
        data = []
        args[0].each do |x|
            if x[:hashring] != nil
                @@message_queue << {
                    :hashring => x[:hashring],
                    :timestamp => Time.now.to_f,
                    :data => x[:data]
                }
            end
            data << x[:data]
            event_list << x[:event]
        end
        @expected_events << event_list
        @data_socket.send(data.join("\n") + "\n", 0, '127.0.0.1', SR_DATA_PORT)
    end

    def run()
        Signal.trap("INT")  { EventMachine.stop }
        Signal.trap("TERM") { EventMachine.stop }
        File.open(SR_CONFIG_FILE, "w") do |f|
            f.puts("data_port=#{SR_DATA_PORT}")
            f.puts("health_port=#{SR_HEALTH_PORT}")
            f.puts("downstream_health_check_interval=#{SR_DS_HEALTH_CHECK_INTERVAL}")
            f.puts("downstream_flush_interval=#{SR_DS_FLUSH_INTERVAL}")
            f.puts("downstream_ping_interval=#{SR_DS_PING_INTERVAL}")
            f.puts("ping_prefix=#{SR_PING_PREFIX}")
            f.puts("downstream=#{(0...DOWNSTREAM_NUM).to_a.map {|x| BASE_DS_PORT + 2 * x}.map {|x| "127.0.0.1:#{x}:#{x + 1}"}.join(',')}")
        end
        @downstream = []
        @data_socket = UDPSocket.new
        EventMachine::run do
            (0...DOWNSTREAM_NUM).each do |i|
                sm = StatsdMock.new(BASE_DS_PORT + 2 * i, BASE_DS_PORT + 2 * i + 1, i, self)
                @downstream << sm
            end
            EventMachine.popen("#{SR_EXE_FILE} #{SR_CONFIG_FILE}", OutputHandler, self)
            EventMachine.add_timer(@timeout) do
                abort("Timeout")
            end
            advance_test_sequence()
        end
    end

    def advance_test_sequence()
        run_data = @test_sequence.shift
        if run_data == nil
            EventMachine.stop()
            exit(SUCCESS_EXIT_STATUS)
        end
        method = run_data[0]
        if method == nil
            abort("No method specified")
        end
        send(method, run_data[1])
    end

    def self.get_message_queue
        @@message_queue
    end

    def initialize()
        @test_sequence = []
        @expected_events = []
        @counter = 0
        @timeout = DEFAULT_TEST_TIMEOUT
    end

    def notify(event)
        if $verbose
            puts "got: #{event}"
        end
        event_list = @expected_events.first
        if $verbose
            puts "waiting for: #{event_list}"
        end
        event_list.each do |e|
            if e[:source] == event[:source] && event[:text].end_with?(e[:text])
                event_list.delete(e)
            end
        end
        if event_list.empty?
            @expected_events.shift
            advance_test_sequence()
        end
    end

    def abort(reason)
        puts "Test failed: #{reason}"
        if EventMachine.reactor_running?
            EventMachine.stop()
        end
        exit(FAILURE_EXIT_STATUS)
    end

    def toggle_ds_impl(ds_list)
        event_list = []
        ds_list.each do |ds_num|
            ds = @downstream[ds_num]
            if ds == nil
                abort("Invalid downstream #{ds_num}")
            end
            text = ds.healthy ? "DEBUG ds_mark_down downstream #{ds_num} is down" : "DEBUG ds_health_read_cb downstream #{ds_num} is up"
            event_list << {source: "statsd-router", text: text}
        end
        @expected_events << event_list
        ds_list.each do |ds_num|
            ds = @downstream[ds_num]
            ds.healthy ? ds.stop() : ds.start()
        end
    end

    def test_sequence
        @test_sequence
    end

    def set_test_timeout(t)
        @timeout = t
    end
end

@srt = StatsdRouterTest.new

$verbose = ARGV.delete("-v")

def set_test_timeout(t)
    @srt.set_test_timeout(t)
end

def valid_metric(n)
    @srt.valid_metric(n)
end

def invalid_metric(n)
    @srt.invalid_metric(n)
end

def toggle_ds(*args)
    @srt.test_sequence << [:toggle_ds_impl, args]
end

def send_data(*args)
    @srt.test_sequence << [:send_data_impl, args]
end

at_exit do
    @srt.run()
end
