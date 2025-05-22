import type { ChartData, ChartOptions } from 'chart.js';
import { Chart, registerables, Tooltip } from 'chart.js';
import { CandlestickController, CandlestickElement } from 'chartjs-chart-financial';
import { Chart as ReactChart } from 'react-chartjs-2';
import { useRef } from 'react';

Chart.register(...registerables, CandlestickController, CandlestickElement, Tooltip);

interface OHLC {
  time: string; // ISO string or date string
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface Props {
  data: OHLC[];
  symbol: string;
}

const StockChart = ({ data, symbol }: Props) => {
  const chartRef = useRef<Chart<'candlestick'>>(null);

  // Convert time to Date objects for Chart.js
  const chartData: ChartData<'candlestick'> = {
    datasets: [
      {
        label: `${symbol} OHLC`,
        data: data.map(d => ({
          x: new Date(d.time).getTime(), // Chart.js expects timestamp (number)
          o: d.open,
          h: d.high,
          l: d.low,
          c: d.close,
        })),
        borderColor: '#4bffb5',
        backgroundColor: '#222',
      }
    ]
  };

  const options: ChartOptions<'candlestick'> = {
    responsive: true,
    plugins: {
      legend: { display: false },
      tooltip: { enabled: true }
    },
    scales: {
      x: {
        type: 'time',
        time: { unit: 'day' },
        ticks: { color: '#fff' }
      },
      y: {
        ticks: { color: '#fff' }
      }
    }
  };

  return (
    <div>
      <h2>{symbol} OHLC Chart</h2>
      <ReactChart
        ref={chartRef}
        type="candlestick"
        data={chartData}
        options={options}
        style={{ background: '#222', padding: 16 }}
      />
    </div>
  );
};

export default StockChart;