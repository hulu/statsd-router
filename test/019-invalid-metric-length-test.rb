#!/usr/bin/env ruby

require './statsd-router-test-lib'

toggle_ds(0, 1)
send_data(invalid_metric(4),
    invalid_metric(1600))

