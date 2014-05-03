#!/usr/bin/env ruby
require 'eventmachine'

DOWNSTREAM_NUM = 3
BASE_DS_PORT = 9100
SR_DATA_PORT = 9000
SR_HEALTH_PORT = 9001
SR_CONFIG_FILE = "/tmp/statsd-router.conf"
SR_EXE_FILE = "./statsd-router"
SR_DS_HEALTH_CHECK_INTERVAL = 3.0
SR_DS_FLUSH_INTERVAL = 2.0
MONKEY_ACTION_INTERVAL_MIN = 0.1
MONKEY_ACTION_INTERVAL_MAX = 2.0
SR_HEALTH_CHECK_REQUEST = "ok"
SR_DS_PING_INTERVAL = 10.0
SR_PING_PREFIX = "statsd-cluster-test"

HEALTH_CHECK_PROBABILITY = 0.05
DS_TOGGLE_PROBABILITY = 0.1

LOG = File.open("/tmp/log", "w")

module Kernel
    alias :oldputs :puts
    def puts(*args)
        oldputs(*args)
        if LOG
            LOG.puts(*args)
        end
    end
end

class DataServer < EventMachine::Connection
    def initialize(statsd_mock)
        @statsd_mock = statsd_mock
    end

    def receive_data(data)
        now = Time.now.to_f
        puts "*** #{@statsd_mock.num} data start"
        data.split("\n").each do |d|
            puts "*** #{d}"
            next if d =~ /^#{SR_PING_PREFIX}/
            m = StatsdRouterMonkey.get_message_queue().select {|x| x[:data] == d}.first
            if m == nil
                puts "!!! Failed to find \"#{d}\" in message queue"
                next
            end
            m[:hashring].each do |h|
                ds = @statsd_mock.get_all[h]
                if ds.num == @statsd_mock.num
                    if !ds.healthy && (now - ds.last_stop_time > SR_DS_HEALTH_CHECK_INTERVAL * 2)
                        puts "!!! #{@statsd_mock.num} got data though it is down"
                    end
                    StatsdRouterMonkey.get_message_queue().delete(m)
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
                puts "!!! Hashring problem: #{m[:hashring]}, #{h} health status: #{ds.healthy}"
            end
        end
        puts "*** #{@statsd_mock.num} data end"
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
    @@all = []

    def initialize(data_port, health_port, num)
        @@all << self
        @num = num
        @data_port = data_port
        @health_port = health_port
        @connections = []
        @last_health_check_time = Time.now.to_f
        @last_start_time = Time.now.to_f
        @last_stop_time = Time.now.to_f
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

module OutputHandler
    def receive_data(data)
        puts data
    end

#    def unbind
#        puts "child process completed"
#        EventMachine.stop()
#    end
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

class StatsdRouterMonkey
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

    def send_data()
        @counter = (@counter + 1) % 1000
        name = "statsd-cluster.count#{rand(100)}"
        data = name + ":#{@counter}|c\n"
        hash = 0
        name.each_byte {|b| hash = (hash *31 + b) & 0xffffffffffffffff}
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
        a.reverse!
        puts "*** #{data.strip} -> #{a}"
        @@message_queue << {:hashring => a, :timestamp => Time.now.to_f, :data => data.strip()}
        @data_socket.send(data, 0, '127.0.0.1', SR_DATA_PORT)
    end

    def toggle_ds()
        healthy_ds_num = @downstream.inject(0) do |r, ds|
            if ds.healthy
                r + 1
            else
                r
            end
        end
        ds = nil
        while ds == nil
            ds = @downstream.shuffle.first
            ds = nil if ds.healthy && healthy_ds_num < 2
        end
        if ds.healthy
            ds.stop()
            puts "*** #{ds.num} stopped"
        else
            ds.start()
            puts "*** #{ds.num} started"
        end
    end

    def monkey_action
        x = rand()
        now = Time.now.to_f
        @downstream.each_with_index do |ds, i|
            if ds.healthy && now - ds.last_health_check_time > SR_DS_HEALTH_CHECK_INTERVAL * 2
                puts "!!! Downstream #{i} health status was checked more than #{SR_DS_HEALTH_CHECK_INTERVAL * 2} seconds ago"
            end
        end
        @@message_queue.each do |m|
            if now - m[:timestamp] > SR_DS_FLUSH_INTERVAL * 2
                should_be_delivered = true
                m[:hashring].map {|h| @downstream[h]}.each do |ds|
                    if m[:timestamp] > ds.last_stop_time && ds.last_stop_time - m[:timestamp] < SR_DS_HEALTH_CHECK_INTERVAL * 2
                        should_be_delivered = false
                        break
                    end
                end
                if should_be_delivered
                    puts "!!! Message #{m[:data]} was not flushed for more than #{SR_DS_FLUSH_INTERVAL * 2} seconds"
                end
                @@message_queue.delete(m)
            end
        end
        if x < HEALTH_CHECK_PROBABILITY
            health_check()
        elsif x < DS_TOGGLE_PROBABILITY
            toggle_ds()
        else
            send_data()
        end
        EventMachine.add_timer(MONKEY_ACTION_INTERVAL_MIN + (MONKEY_ACTION_INTERVAL_MAX - MONKEY_ACTION_INTERVAL_MIN) * rand()) { monkey_action() }
    end

    def run
        EventMachine::run do
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
            EventMachine.popen("#{SR_EXE_FILE} #{SR_CONFIG_FILE}", OutputHandler)
            @downstream = []
            (0...DOWNSTREAM_NUM).each do |i|
                sm = StatsdMock.new(BASE_DS_PORT + 2 * i, BASE_DS_PORT + 2 * i + 1, i)
                @downstream << sm
                sm.start()
            end
            @counter = 0
            @data_socket = UDPSocket.new
            monkey_action()
        end
    end

    def self.get_message_queue
        @@message_queue
    end
end

StatsdRouterMonkey.new.run
