#!/usr/bin/env ruby

require './statsd-router-test-lib'

toggle_ds(0, 1)
send_data(valid_metric(128), invalid_metric(128), valid_metric(128))
toggle_ds(0, 2)
send_data(invalid_metric(128), valid_metric(128), invalid_metric(128))
toggle_ds(0, 1)
send_data(valid_metric(128), invalid_metric(128), valid_metric(128))

