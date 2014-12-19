#!/usr/bin/env ruby

# This is library for black box testing of statsd-router

require 'eventmachine'

# how many statsd instances we want to emulate
DOWNSTREAM_NUM = 3
# statsd instances would listen on ports (9100 - data, 9101 - health), (9102 - data, 9103 - health) etc
BASE_DS_PORT = 9100
# data port for statsd router
SR_DATA_PORT = 9000
# health port for statsd router
SR_HEALTH_PORT = 9001
# location of config file for statsd router. THis config file is generated for each test run.
SR_CONFIG_FILE = "/tmp/statsd-router.conf"
# location of statsd router executable
SR_EXE_FILE = "../statsd-router"
# how often statsd router checks health of downstreams
SR_DS_HEALTH_CHECK_INTERVAL = 3.0
# how often statsd router flushes data to downstreams
SR_DS_FLUSH_INTERVAL = 2.0
# health check request for statsd router
SR_HEALTH_CHECK_REQUEST = "ok"
# how often statsd router pushes internal metric for data loss detection
SR_DS_PING_INTERVAL = 10.0
# prefix for internal metric for data loss detection
SR_PING_PREFIX = "statsd-cluster-test"

# test exit code in case of success
SUCCESS_EXIT_STATUS = 0
# test exit code in case of failure
FAILURE_EXIT_STATUS = 1
# default test timeout value, can be changed via set_test_timeout() method
DEFAULT_TEST_TIMEOUT = 20

# min and max metrics length (those are processed differently than metrics with garbage content)
MIN_METRICS_LENGTH = 6
MAX_METRICS_LENGTH = 1450

# DataServer is part of statsd simulation. It listens on UDP port, accepts data,
# and verifies that it got expected metrics
class DataServer < EventMachine::Connection
    def initialize(statsd_mock)
        @statsd_mock = statsd_mock
        @test_controller = statsd_mock.test_controller
    end

    # this method is called when UDP socket gets data
    def receive_data(data)
        now = Time.now.to_f
        # packet can contain several metrics separated by new lines, let's process them one by one
        data.split("\n").each do |d|
            # internal metric for data loss detection is ignored
            next if d =~ /^#{SR_PING_PREFIX}/
            # let's find metric we've got in message queue
            m = StatsdRouterTest.get_message_queue().select {|x| x[:data] == d}.first
            # if metric was not found - this is error, test should be aborted
            if m == nil
                @test_controller.abort("Failed to find \"#{d}\" in message queue")
            end
            # now let's check that metric was delivered to correct downstream according to consistent hashing
            # m[:hashring] contains statsd instance numbers
            m[:hashring].each do |h|
                # we retrieve downstream
                ds = @statsd_mock.get_all[h]
                # and check its index
                # if index equals to index of statsd, which received this metric
                if ds.num == @statsd_mock.num
                    # we check if this statsd is up, giving it some free play range
                    # if statsd is down, but it received metric - this is an error, test should be aborted
                    if !ds.healthy && (now - ds.last_stop_time > SR_DS_HEALTH_CHECK_INTERVAL * 2)
                        @test_controller.abort("#{@statsd_mock.num} got data though it is down")
                    end
                    # let's remove received metric from message queue
                    StatsdRouterTest.get_message_queue().delete(m)
                    break
                else
                    # we are here because ds is not downstream, which received metric
                    # if it is down - no issues
                    # if it is up we need to check, when it became alive and give it some free play range
                    if ds.healthy
                        if m[:timestamp] > ds.last_start_time && m[:timestamp] - ds.last_start_time < SR_DS_HEALTH_CHECK_INTERVAL * 2
                            next
                        end
                    else
                        next
                    end
                end
                # something is wrong, metric was received by wrong downstream or was not received by correct one
                # this is error, test should be aborted
                @test_controller.abort("Hashring problem: #{m[:hashring]}, #{h} health status: #{ds.healthy}")
            end
            # let's notify test controller, that metric was delivered successfully
            @test_controller.notify({source: "statsd", text: d})
        end
    end
end

# health server for statsd instance
# responds with "health: up" on health requests like real statsd
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

# umbrella class, using DataServer and HealthServer
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

# helper class to handle statsd router console output
class OutputHandler < EM::Connection
    def initialize(test_controller)
        @test_controller = test_controller
    end

    # when data is received test controller is being notified
    def receive_data(data)
        data.split("\n").each do |d|
            @test_controller.notify({source: "statsd-router", text: d})
        end
    end
end

# module to check statsd router health
# currently statsd router health check is done via TCP port
# request is echoed back
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

    # function to check statsd router health
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

    # function to calculate consistent hashing ring
    # same algorithms are used in statsd router
    def hashring(name)
        # 1st we calculate hash name for the name (algorithm borrowed from java String class)
        hash = 0
        name.each_byte {|b| hash = (hash * 31 + b) & 0xffffffffffffffff}
        # next we create array with downstream numbers and shuffle it using hash value
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

    # this function generates valid metric of given length
    def valid_metric(length)
        name = "statsd-cluster.count"
        number = rand(100).to_s
        name_length = name.length + number.length
        if name_length < length
            name += ("X" * (length - name_length) + number)
        end
        a = hashring(name)
        # to simplify metric identification counter value grows from 0 to 1000, after that is reset back to 0 and so on
        @counter = (@counter + 1) % 1000
        # only counters are generated
        data = name + ":#{@counter}|c"
        # we return data necessary to register metric in message queue and expected events
        {
            hashring: a,
            data: data,
            event: {source: "statsd", text: data}
        }
    end

    # this function generates invalid metrics of given length
    def invalid_metric(length)
        # length - 1 because of terminating new line
        data = (0...(length - 1)).map { (65 + rand(26)).chr }.join
        if length < MIN_METRICS_LENGTH || length > MAX_METRICS_LENGTH
            {
                data: data,
                event: {source: "statsd-router", text: "WARN udp_read_cb: invalid length #{length} of metric #{data}"}
            }
        else
            {
                data: data,
                event: {source: "statsd-router", text: "WARN process_data_line: invalid metric #{data}"}
            }
        end
    end

    # this function sends data during test execution
    def send_data_impl(*args)
        event_list = []
        data = []
        args[0].each do |x|
            # if hashring is not nil this is valid metric
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

    # this function runs actual test
    def run()
        # let's install signal handlers
        Signal.trap("INT")  { EventMachine.stop }
        Signal.trap("TERM") { EventMachine.stop }
        # let's generate config file for statsd router
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
        # socket for sending data
        @data_socket = UDPSocket.new
        # here we start event machine
        EventMachine::run do
            # let's init downstreams
            (0...DOWNSTREAM_NUM).each do |i|
                sm = StatsdMock.new(BASE_DS_PORT + 2 * i, BASE_DS_PORT + 2 * i + 1, i, self)
                @downstream << sm
            end
            # start statsd router
            EventMachine.popen("#{SR_EXE_FILE} #{SR_CONFIG_FILE}", OutputHandler, self)
            # and set timer to interrupt test in case of timeout
            EventMachine.add_timer(@timeout) do
                abort("Timeout")
            end
            advance_test_sequence()
        end
    end

    # this function goes through test sequence
    def advance_test_sequence()
        run_data = @test_sequence.shift
        # if test sequence is empty test is completed, let's report success
        if run_data == nil
            EventMachine.stop()
            exit(SUCCESS_EXIT_STATUS)
        end
        # otherwise let's run next test step
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

    # this function is used to notify test of external events
    def notify(event)
        if $verbose
            puts "got: #{event}"
        end
        # this is list of events we expect
        event_list = @expected_events.first
        if $verbose
            puts "waiting for: #{event_list}"
        end
        # if we've got expected event we remove it from the list
        event_list.each do |e|
            if e[:source] == event[:source] && event[:text].end_with?(e[:text])
                event_list.delete(e)
            end
        end
        # in case of no more events to expect this test step is done, we can run next step
        if event_list.empty?
            @expected_events.shift
            advance_test_sequence()
        end
    end

    # this function is used to stop test run in case of error
    def abort(reason)
        puts "Test failed: #{reason}"
        if EventMachine.reactor_running?
            EventMachine.stop()
        end
        exit(FAILURE_EXIT_STATUS)
    end

    # function to toggle downsream state
    def toggle_ds_impl(ds_list)
        event_list = []
        ds_list.each do |ds_num|
            ds = @downstream[ds_num]
            if ds == nil
                abort("Invalid downstream #{ds_num}")
            end
            text = ds.healthy ? "TRACE ds_mark_down downstream #{ds_num} is down" : "TRACE ds_health_read_cb downstream #{ds_num} is up"
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

# syntactic sugar start

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

# syntactic sugar end

# test configuration is done, now let's run it
at_exit do
    @srt.run()
end
