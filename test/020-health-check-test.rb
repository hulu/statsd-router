#!/usr/bin/env ruby

require './statsd-router-test-lib'

health_check('')
health_check('up')
health_check('down')
health_check('')
health_check('down')
health_check('up')
health_check('')
