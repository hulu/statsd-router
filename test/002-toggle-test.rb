#!/usr/bin/env ruby

require './statsd-router-test-lib'

toggle_ds(0, 1, 2)
toggle_ds(2)
toggle_ds(0)
toggle_ds(1)
