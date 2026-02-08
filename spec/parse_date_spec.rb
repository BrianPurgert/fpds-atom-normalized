require_relative '../lib/parsers'

describe 'parse_date' do
  it 'returns a Date object when given a valid string' do
    expect(Parsers.parse_date('2024-01-01')).to eq(Date.new(2024,1,1))
  end

  it 'returns nil for blank input' do
    expect(Parsers.parse_date('')).to be_nil
  end
end
