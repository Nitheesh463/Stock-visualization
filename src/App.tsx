import { useEffect, useState } from 'react';
import axios from 'axios';
import StockChart from './components/StockChart';

interface OHLC {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

function App() {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [selectedSymbol, setSelectedSymbol] = useState<string>('');
  const [timeframe, setTimeframe] = useState<'daily' | 'weekly' | 'monthly'>('daily');
  const [data, setData] = useState<OHLC[]>([]);

  useEffect(() => {
    axios.get('http://localhost:5000/api/companies')
      .then(res => {
        setSymbols(res.data);
        if (res.data.length) setSelectedSymbol(res.data[0]);
      });
  }, []);

  useEffect(() => {
    if (!selectedSymbol) return;
    axios.get(`http://localhost:5000/api/ohlc/${selectedSymbol}/${timeframe}`)
      .then(res => setData(res.data));
  }, [selectedSymbol, timeframe]);

  return (
    <div style={{ maxWidth: 900, margin: '0 auto', padding: 24 }}>
      <h1>Stock Visualization</h1>
      <div style={{ marginBottom: 16 }}>
        <select value={selectedSymbol} onChange={e => setSelectedSymbol(e.target.value)}>
          {symbols.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
        <select value={timeframe} onChange={e => setTimeframe(e.target.value as 'daily' | 'weekly' | 'monthly')} style={{ marginLeft: 8 }}>
          <option value="daily">Daily</option>
          <option value="weekly">Weekly</option>
          <option value="monthly">Monthly</option>
        </select>
      </div>
      {data.length > 0 && <StockChart data={data} symbol={selectedSymbol} />}
    </div>
  );
}

export default App;