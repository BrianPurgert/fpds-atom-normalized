require 'json'
require 'date'
require 'open3'

describe 'generate_date_chunks' do
  let(:script) { File.expand_path('../scripts/generate_date_chunks.rb', __dir__) }

  def run_chunks(start_date, end_date, num_chunks)
    stdout, status = Open3.capture2('ruby', script, start_date, end_date, num_chunks.to_s)
    expect(status.success?).to be true
    JSON.parse(stdout)
  end

  it 'produces the requested number of chunks' do
    chunks = run_chunks('2020-01-01', '2020-12-31', 10)
    expect(chunks.size).to eq(10)
  end

  it 'covers the full date range without gaps or overlaps' do
    chunks = run_chunks('2020-01-01', '2020-12-31', 10)

    expect(chunks.first['start_date']).to eq('2020-01-01')
    expect(chunks.last['end_date']).to eq('2020-12-31')

    chunks.each_cons(2) do |a, b|
      a_end   = Date.parse(a['end_date'])
      b_start = Date.parse(b['start_date'])
      expect(b_start).to eq(a_end + 1), "Gap between chunk #{a['chunk']} and #{b['chunk']}"
    end
  end

  it 'caps chunks to available days when num_chunks exceeds total days' do
    chunks = run_chunks('2025-01-01', '2025-01-03', 10)
    expect(chunks.size).to eq(3) # only 3 days available
    expect(chunks.map { |c| c['start_date'] }).to eq(%w[2025-01-01 2025-01-02 2025-01-03])
    expect(chunks.map { |c| c['end_date'] }).to eq(%w[2025-01-01 2025-01-02 2025-01-03])
  end

  it 'works with a single chunk' do
    chunks = run_chunks('2025-06-01', '2025-06-30', 1)
    expect(chunks.size).to eq(1)
    expect(chunks[0]['start_date']).to eq('2025-06-01')
    expect(chunks[0]['end_date']).to eq('2025-06-30')
  end

  it 'numbers chunks sequentially from 1' do
    chunks = run_chunks('2020-01-01', '2020-12-31', 5)
    expect(chunks.map { |c| c['chunk'] }).to eq(%w[1 2 3 4 5])
  end
end
