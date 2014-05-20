#!/usr/bin/env ruby

require './statsd-router-test-lib'

toggle_ds(0, 1, 2)
send_data(valid_metric(64))
