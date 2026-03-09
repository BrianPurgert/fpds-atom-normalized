#!/usr/bin/env ruby
# Splits a date range into N equal chunks and outputs JSON for GitHub Actions matrix.
#
# Usage:
#   ruby scripts/generate_date_chunks.rb START_DATE END_DATE NUM_CHUNKS
#
# Example:
#   ruby scripts/generate_date_chunks.rb 1998-01-01 2025-12-31 100
#
# Output (JSON array):
#   [{"start_date":"1998-01-01","end_date":"1998-04-11","chunk":"1"},...]

require 'date'
require 'json'

start_date  = Date.parse(ARGV[0])
end_date    = Date.parse(ARGV[1])
num_chunks  = [ARGV[2].to_i, 1].max

total_days  = (end_date - start_date).to_i + 1
num_chunks  = [num_chunks, total_days].min # can't have more chunks than days

chunk_size  = total_days / num_chunks
remainder   = total_days % num_chunks

chunks = []
cursor = start_date

num_chunks.times do |i|
  # Distribute remainder days across the first `remainder` chunks
  days_in_chunk = chunk_size + (i < remainder ? 1 : 0)
  chunk_end = cursor + days_in_chunk - 1

  chunks << {
    start_date: cursor.iso8601,
    end_date:   chunk_end.iso8601,
    chunk:      (i + 1).to_s
  }

  cursor = chunk_end + 1
end

puts JSON.generate(chunks)
