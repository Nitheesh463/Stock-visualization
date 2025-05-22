import pandas as pd
from sqlalchemy import create_engine, Column, String, Date, Numeric, BigInteger, Index, text, func
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert
from flask import Flask, jsonify
import chardet
from datetime import datetime, timedelta
import logging
from dateutil.parser import parse as dateutil_parse
from flask_cors import CORS

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database Setup
DATABASE_URL = "postgresql://postgres:nse123@localhost:5433/nse_db"
engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Models
class StockData(Base):
    __tablename__ = 'stock_data'
    symbol = Column(String(50), primary_key=True)
    date = Column(Date, primary_key=True)
    series = Column(String(10))
    open_price = Column(Numeric(10, 2))
    high_price = Column(Numeric(10, 2))
    low_price = Column(Numeric(10, 2))
    close_price = Column(Numeric(10, 2))
    prev_close = Column(Numeric(10, 2))
    ttl_trd_qnty = Column(BigInteger)
    __table_args__ = (
        Index('idx_stock_data_date', 'date'),
        Index('idx_stock_data_symbol', 'symbol'),
    )

class StockOHLCAggregated(Base):
    __tablename__ = 'stock_ohlc_aggregated'
    symbol = Column(String(50), primary_key=True)
    timeframe = Column(String(10), primary_key=True)
    start_date = Column(Date, primary_key=True)
    end_date = Column(Date)
    open_price = Column(Numeric(10, 2))
    high_price = Column(Numeric(10, 2))
    low_price = Column(Numeric(10, 2))
    close_price = Column(Numeric(10, 2))
    volume = Column(BigInteger)
    __table_args__ = (Index('idx_stock_ohlc_aggregated_end_date', 'end_date'),)

class CompanyMetadata(Base):
    __tablename__ = 'company_metadata'
    symbol = Column(String(50), primary_key=True)
    inception_date = Column(Date)
    last_trading_date = Column(Date)
    company_name = Column(String(255))
    __table_args__ = (Index('idx_company_metadata_dates', 'inception_date', 'last_trading_date'),)

class PivotPoints(Base):
    __tablename__ = 'pivot_points'
    symbol = Column(String(50), primary_key=True)
    timeframe = Column(String(10), primary_key=True)
    date = Column(Date, primary_key=True)
    pivot_point = Column(Numeric(10, 2))
    r1 = Column(Numeric(10, 2))
    s1 = Column(Numeric(10, 2))
    r2 = Column(Numeric(10, 2))
    s2 = Column(Numeric(10, 2))
    r3 = Column(Numeric(10, 2))
    s3 = Column(Numeric(10, 2))
    __table_args__ = (Index('idx_pivot_points_date', 'date'),)

# Create Tables
Base.metadata.create_all(engine)

# Flask App
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "http://localhost:5173"}})  # Updated CORS for Vite frontend

# Detect File Encoding
def detect_file_encoding(file_path):
    try:
        with open(file_path, 'rb') as file:
            result = chardet.detect(file.read(30000))
        encoding = result['encoding']
        if encoding is None:
            logger.warning(f"Could not detect encoding for {file_path}, defaulting to utf-8")
            encoding = 'utf-8'
        logger.info(f"Detected encoding for {file_path}: {encoding}")
        return encoding
    except Exception as e:
        logger.error(f"Error detecting file encoding: {e}")
        raise

# Verify Table Existence
def verify_tables():
    with Session() as session:
        tables = ['stock_data', 'stock_ohlc_aggregated', 'company_metadata', 'pivot_points']
        for table in tables:
            exists = session.execute(
                text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}')")
            ).scalar()
            logger.info(f"Table {table} exists: {exists}")
            if not exists:
                raise Exception(f"Table {table} does not exist in the database")

# Custom Date Parsing Function
def parse_date(date_str):
    if pd.isna(date_str) or not isinstance(date_str, str):
        return None
    date_formats = [
        '%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y',
        '%Y/%m/%d', '%d.%m.%Y', '%Y.%m.%d', '%m-%d-%Y',
        '%d %b %Y', '%d %B %Y', '%Y %b %d', '%Y %B %d',
        '%Y%m%d', '%d%m%Y', '%m%d%Y', '%Y%m%d%H%M%S',
        '%d-%b-%Y', '%d-%B-%Y', '%Y-%b-%d', '%Y-%B-%d'
    ]
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    try:
        return dateutil_parse(date_str, fuzzy=True).date()
    except ValueError:
        return None

# Process Combined CSV in Chunks
def process_combined_csv(file_path, chunk_size=30000):
    logger.info(f"Starting data processing at {datetime.now()}")
    with Session() as session:
        try:
            encoding = detect_file_encoding(file_path)
            required_columns = ['SYMBOL', 'DATE1', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'TTL_TRD_QNTY']
            total_rows_read = 0
            total_rows_dropped_dates = 0
            total_rows_dropped_numeric = 0
            total_rows_dropped_duplicates = 0
            total_rows_skipped_conflicts = 0
            total_rows_inserted = 0
            invalid_date_rows = []
            invalid_numeric_rows = []

            for chunk in pd.read_csv(file_path, encoding=encoding, chunksize=chunk_size, low_memory=True):
                logger.info(f"Processing chunk of {len(chunk)} rows")
                total_rows_read += len(chunk)
                chunk.columns = [col.strip().upper() for col in chunk.columns]
                if not all(col in chunk.columns for col in required_columns):
                    missing = set(required_columns) - set(chunk.columns)
                    logger.error(f"Missing required columns: {missing}")
                    raise Exception(f"Missing required columns: {missing}")

                chunk['DATE1'] = chunk['DATE1'].apply(parse_date)
                invalid_dates = chunk[chunk['DATE1'].isna()]
                if not invalid_dates.empty:
                    invalid_date_rows.append(invalid_dates)
                    total_rows_dropped_dates += len(invalid_dates)
                    logger.warning(f"Found {len(invalid_dates)} rows with invalid dates in this chunk")

                chunk_before = len(chunk)
                chunk = chunk.drop_duplicates(subset=['SYMBOL', 'DATE1'], keep='first')
                dropped_duplicates = chunk_before - len(chunk)
                total_rows_dropped_duplicates += dropped_duplicates
                if dropped_duplicates > 0:
                    logger.info(f"Dropped {dropped_duplicates} duplicate rows in this chunk")

                numeric_cols = ['OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'TTL_TRD_QNTY']
                chunk_before = len(chunk)
                for col in numeric_cols:
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                invalid_numeric = chunk[chunk[numeric_cols].isna().any(axis=1)]
                if not invalid_numeric.empty:
                    invalid_numeric_rows.append(invalid_numeric)
                    total_rows_dropped_numeric += len(invalid_numeric)
                    logger.warning(f"Found {len(invalid_numeric)} rows with invalid numeric values in this chunk")
                chunk = chunk.dropna(subset=numeric_cols)

                if chunk.empty:
                    logger.warning("Chunk is empty after cleaning, skipping")
                    continue

                stock_data = chunk[required_columns + ['SERIES', 'PREV_CLOSE']].rename(columns={
                    'SYMBOL': 'symbol', 'DATE1': 'date', 'SERIES': 'series',
                    'OPEN_PRICE': 'open_price', 'HIGH_PRICE': 'high_price',
                    'LOW_PRICE': 'low_price', 'CLOSE_PRICE': 'close_price',
                    'PREV_CLOSE': 'prev_close', 'TTL_TRD_QNTY': 'ttl_trd_qnty'
                }).to_dict('records')

                result = session.execute(
                    insert(StockData.__table__).values(stock_data).on_conflict_do_nothing(
                        index_elements=['symbol', 'date']
                    )
                )
                inserted_rows = result.rowcount
                skipped_rows = len(stock_data) - inserted_rows
                total_rows_inserted += inserted_rows
                total_rows_skipped_conflicts += skipped_rows
                logger.info(f"Inserted {inserted_rows} rows, skipped {skipped_rows} due to conflicts in this chunk. Total inserted: {total_rows_inserted}")

                company_data = chunk.groupby('SYMBOL').agg({'DATE1': ['min', 'max']}).reset_index()
                company_data.columns = ['symbol', 'inception_date', 'last_trading_date']
                company_records = [
                    {'symbol': row['symbol'], 'inception_date': row['inception_date'],
                     'last_trading_date': row['last_trading_date'], 'company_name': row['symbol']}
                    for _, row in company_data.iterrows() if row['inception_date'] is not None
                ]
                session.execute(
                    insert(CompanyMetadata.__table__).values(company_records).on_conflict_do_nothing(
                        index_elements=['symbol']
                    )
                )

            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            if invalid_date_rows:
                pd.concat(invalid_date_rows).to_csv(f"invalid_dates_{timestamp}.csv", index=False)
                logger.info(f"Saved {total_rows_dropped_dates} rows with invalid dates to invalid_dates_{timestamp}.csv")
            if invalid_numeric_rows:
                pd.concat(invalid_numeric_rows).to_csv(f"invalid_numeric_{timestamp}.csv", index=False)
                logger.info(f"Saved {total_rows_dropped_numeric} rows with invalid numeric values to invalid_numeric_{timestamp}.csv")

            logger.info("Processing complete. Summary:")
            logger.info(f"Total rows read: {total_rows_read}")
            logger.info(f"Rows with invalid dates: {total_rows_dropped_dates}")
            logger.info(f"Rows with invalid numeric values: {total_rows_dropped_numeric}")
            logger.info(f"Rows dropped as duplicates: {total_rows_dropped_duplicates}")
            logger.info(f"Rows skipped due to conflicts: {total_rows_skipped_conflicts}")
            logger.info(f"Total rows inserted into stock_data: {total_rows_inserted}")

            accounted_rows = (total_rows_inserted + total_rows_dropped_dates +
                             total_rows_dropped_numeric + total_rows_dropped_duplicates +
                             total_rows_skipped_conflicts)
            if accounted_rows == total_rows_read:
                logger.info("All rows accounted for.")
            else:
                logger.warning(f"Mismatch in row counts. Expected {total_rows_read}, accounted for {accounted_rows}")

            session.commit()

        except Exception as e:
            session.rollback()
            logger.error(f"Error processing CSV: {e}")
            raise

# Aggregate OHLC
def aggregate_ohlc():
    logger.info("Starting OHLC aggregation")
    with Session() as session:
        try:
            latest_date = session.execute(
                text("SELECT MAX(date) FROM stock_data")
            ).scalar()
            if latest_date:
                min_date = latest_date - pd.Timedelta(days=30)
                logger.info(f"Processing data from {min_date} to {latest_date}")
            else:
                min_date = None
                logger.info("No data in stock_data, processing all data")

            query_weekly = """
                WITH date_bounds AS (
                    SELECT 
                        symbol,
                        DATE_TRUNC('week', date) AS start_date,
                        MIN(date) AS first_date,
                        MAX(date) AS last_date
                    FROM stock_data
                    {}
                    GROUP BY symbol, DATE_TRUNC('week', date)
                )
                INSERT INTO stock_ohlc_aggregated (symbol, timeframe, start_date, end_date, open_price, high_price, low_price, close_price, volume)
                SELECT 
                    d.symbol,
                    'weekly' AS timeframe,
                    d.start_date,
                    d.start_date + INTERVAL '6 days' AS end_date,
                    (SELECT open_price FROM stock_data s WHERE s.symbol = d.symbol AND s.date = d.first_date) AS open_price,
                    MAX(s.high_price) AS high_price,
                    MIN(s.low_price) AS low_price,
                    (SELECT close_price FROM stock_data s WHERE s.symbol = d.symbol AND s.date = d.last_date) AS close_price,
                    SUM(s.ttl_trd_qnty) AS volume
                FROM stock_data s
                JOIN date_bounds d ON s.symbol = d.symbol AND DATE_TRUNC('week', s.date) = d.start_date
                {}
                GROUP BY d.symbol, d.start_date, d.first_date, d.last_date
                ON CONFLICT (symbol, timeframe, start_date) DO NOTHING
            """
            if min_date:
                query_weekly = query_weekly.format("WHERE date >= :min_date", "WHERE s.date >= :min_date")
                session.execute(text(query_weekly), {'min_date': min_date})
            else:
                query_weekly = query_weekly.format("", "")
                session.execute(text(query_weekly))

            query_monthly = """
                WITH date_bounds AS (
                    SELECT 
                        symbol,
                        DATE_TRUNC('month', date) AS start_date,
                        MIN(date) AS first_date,
                        MAX(date) AS last_date
                    FROM stock_data
                    {}
                    GROUP BY symbol, DATE_TRUNC('month', date)
                )
                INSERT INTO stock_ohlc_aggregated (symbol, timeframe, start_date, end_date, open_price, high_price, low_price, close_price, volume)
                SELECT 
                    d.symbol,
                    'monthly' AS timeframe,
                    d.start_date,
                    d.start_date + INTERVAL '1 month - 1 day' AS end_date,
                    (SELECT open_price FROM stock_data s WHERE s.symbol = d.symbol AND s.date = d.first_date) AS open_price,
                    MAX(s.high_price) AS high_price,
                    MIN(s.low_price) AS low_price,
                    (SELECT close_price FROM stock_data s WHERE s.symbol = d.symbol AND s.date = d.last_date) AS close_price,
                    SUM(s.ttl_trd_qnty) AS volume
                FROM stock_data s
                JOIN date_bounds d ON s.symbol = d.symbol AND DATE_TRUNC('month', s.date) = d.start_date
                {}
                GROUP BY d.symbol, d.start_date, d.first_date, d.last_date
                ON CONFLICT (symbol, timeframe, start_date) DO NOTHING
            """
            if min_date:
                query_monthly = query_monthly.format("WHERE date >= :min_date", "WHERE s.date >= :min_date")
                session.execute(text(query_monthly), {'min_date': min_date})
            else:
                query_monthly = query_monthly.format("", "")
                session.execute(text(query_monthly))

            session.commit()
            logger.info("OHLC aggregation completed")

        except Exception as e:
            session.rollback()
            logger.error(f"Error aggregating OHLC: {e}")
            raise

# Calculate Pivot Points
def calculate_pivot_points():
    logger.info("Starting pivot points calculation")
    with Session() as session:
        try:
            latest_date = session.execute(
                text("SELECT MAX(date) FROM stock_data")
            ).scalar()
            if latest_date:
                min_date = latest_date - pd.Timedelta(days=30)
                logger.info(f"Calculating pivot points from {min_date} to {latest_date}")
            else:
                min_date = None
                logger.info("No data in stock_data, calculating all pivot points")

            query_daily = """
                INSERT INTO pivot_points (symbol, timeframe, date, pivot_point, r1, s1, r2, s2, r3, s3)
                SELECT 
                    symbol, 
                    'daily', 
                    date, 
                    (high_price + low_price + close_price) / 3 AS pivot_point,
                    (2 * ((high_price + low_price + close_price) / 3) - low_price) AS r1,
                    (2 * ((high_price + low_price + close_price) / 3) - high_price) AS s1,
                    ((high_price + low_price + close_price) / 3) + (high_price - low_price) AS r2,
                    ((high_price + low_price + close_price) / 3) - (high_price - low_price) AS s2,
                    high_price + 2 * (((high_price + low_price + close_price) / 3) - low_price) AS r3,
                    low_price - 2 * (high_price - ((high_price + low_price + close_price) / 3)) AS s3
                FROM stock_data
                {}
                ON CONFLICT (symbol, timeframe, date) DO NOTHING
            """
            if min_date:
                query_daily = query_daily.format("WHERE date >= :min_date")
                session.execute(text(query_daily), {'min_date': min_date})
            else:
                query_daily = query_daily.format("")
                session.execute(text(query_daily))

            query_agg = """
                INSERT INTO pivot_points (symbol, timeframe, date, pivot_point, r1, s1, r2, s2, r3, s3)
                SELECT 
                    symbol, 
                    timeframe, 
                    start_date AS date, 
                    (high_price + low_price + close_price) / 3 AS pivot_point,
                    (2 * ((high_price + low_price + close_price) / 3) - low_price) AS r1,
                    (2 * ((high_price + low_price + close_price) / 3) - high_price) AS s1,
                    ((high_price + low_price + close_price) / 3) + (high_price - low_price) AS r2,
                    ((high_price + low_price + close_price) / 3) - (high_price - low_price) AS s2,
                    high_price + 2 * (((high_price + low_price + close_price) / 3) - low_price) AS r3,
                    low_price - 2 * (high_price - ((high_price + low_price + close_price) / 3)) AS s3
                FROM stock_ohlc_aggregated
                {}
                ON CONFLICT (symbol, timeframe, date) DO NOTHING
            """
            if min_date:
                query_agg = query_agg.format("WHERE start_date >= :min_date")
                session.execute(text(query_agg), {'min_date': min_date})
            else:
                query_agg = query_agg.format("")
                session.execute(text(query_agg))

            session.commit()
            logger.info("Pivot points calculation completed")

        except Exception as e:
            session.rollback()
            logger.error(f"Error calculating pivot points: {e}")
            raise

# Flask API Endpoints
@app.route('/api/ohlc/<symbol>/<timeframe>', methods=['GET'])
def get_ohlc(symbol, timeframe):
    with Session() as session:
        try:
            timeframe = timeframe.lower()
            if timeframe not in ['daily', 'weekly', 'monthly']:
                return jsonify({'error': 'Invalid timeframe'}), 400

            days = 365 if timeframe != 'monthly' else 5 * 365
            min_date = datetime.now().date() - pd.Timedelta(days=days)

            if timeframe == 'daily':
                result = session.query(StockData).filter(
                    func.upper(StockData.symbol) == symbol.upper(),
                    StockData.date >= min_date
                ).order_by(StockData.date).all()
                data = [{
                    'time': r.date.isoformat(),
                    'open': float(r.open_price),
                    'high': float(r.high_price),
                    'low': float(r.low_price),
                    'close': float(r.close_price),
                    'volume': r.ttl_trd_qnty
                } for r in result]
            else:
                result = session.query(StockOHLCAggregated).filter(
                    func.upper(StockOHLCAggregated.symbol) == symbol.upper(),
                    func.upper(StockOHLCAggregated.timeframe) == timeframe.upper(),
                    StockOHLCAggregated.start_date >= min_date
                ).order_by(StockOHLCAggregated.start_date).all()
                data = [{
                    'time': r.start_date.isoformat(),
                    'open': float(r.open_price),
                    'high': float(r.high_price),
                    'low': float(r.low_price),
                    'close': float(r.close_price),
                    'volume': r.volume
                } for r in result]

            logger.info(f"Retrieved {len(data)} OHLC records for {symbol}/{timeframe}")
            return jsonify(data)

        except Exception as e:
            logger.error(f"Error in get_ohlc: {e}")
            return jsonify({'error': str(e)}), 500

@app.route('/api/pivot_points/<symbol>/<timeframe>', methods=['GET'])
def get_pivot_points(symbol, timeframe):
    with Session() as session:
        try:
            timeframe = timeframe.lower()
            if timeframe not in ['daily', 'weekly', 'monthly']:
                return jsonify({'error': 'Invalid timeframe'}), 400

            days = 365 if timeframe != 'monthly' else 5 * 365
            min_date = datetime.now().date() - pd.Timedelta(days=days)

            result = session.query(PivotPoints).filter(
                func.upper(PivotPoints.symbol) == symbol.upper(),
                func.upper(PivotPoints.timeframe) == timeframe.upper(),
                PivotPoints.date >= min_date
            ).order_by(PivotPoints.date).all()
            data = [{
                'time': r.date.isoformat(),
                'pivot_point': float(r.pivot_point),
                'r1': float(r.r1), 's1': float(r.s1),
                'r2': float(r.r2), 's2': float(r.s2),
                'r3': float(r.r3), 's3': float(r.s3)
            } for r in result]

            logger.info(f"Retrieved {len(data)} pivot points for {symbol}/{timeframe}")
            return jsonify(data)

        except Exception as e:
            logger.error(f"Error in get_pivot_points: {e}")
            return jsonify({'error': str(e)}), 500

@app.route('/api/companies', methods=['GET'])
def get_companies():
    with Session() as session:
        try:
            result = session.execute(text("""
                SELECT symbol 
                FROM company_metadata 
                WHERE last_trading_date >= inception_date + INTERVAL '6 months'
            """)).fetchall()
            companies = [row.symbol for row in result]
            logger.info(f"Retrieved {len(companies)} companies")
            return jsonify(companies)

        except Exception as e:
            logger.error(f"Error in get_companies: {e}")
            return jsonify({'error': str(e)}), 500

@app.route('/api/company_data/<symbol>/<date>', methods=['GET'])
def get_company_data(symbol, date):
    with Session() as session:
        try:
            chosen_date = datetime.strptime(date, '%Y-%m-%d').date()
            six_months_ago = chosen_date - timedelta(days=180)

            trade_check = session.query(
                StockData.symbol
            ).filter(
                func.upper(StockData.symbol) == symbol.upper(),
                StockData.date == chosen_date
            ).first()
            if not trade_check:
                return jsonify({'error': 'Company did not trade on the selected date'}), 404

            result = session.query(
                StockData.date,
                StockData.open_price,
                StockData.high_price,
                StockData.low_price,
                StockData.close_price,
                StockData.ttl_trd_qnty
            ).filter(
                func.upper(StockData.symbol) == symbol.upper(),
                StockData.date >= six_months_ago,
                StockData.date <= chosen_date
            ).order_by(StockData.date).all()

            data = [{
                'date': r.date.isoformat(),
                'open': float(r.open_price),
                'high': float(r.high_price),
                'low': float(r.low_price),
                'close': float(r.close_price),
                'volume': r.ttl_trd_qnty
            } for r in result]

            logger.info(f"Retrieved {len(data)} records for {symbol} up to {date}")
            return jsonify(data)

        except ValueError:
            logger.error(f"Invalid date format: {date}")
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        except Exception as e:
            logger.error(f"Error in get_company_data: {e}")
            return jsonify({'error': str(e)}), 500

@app.route('/api/traded_companies/<date>', methods=['GET'])
def get_traded_companies(date):
    with Session() as session:
        try:
            chosen_date = datetime.strptime(date, '%Y-%m-%d').date()

            result = session.query(StockData.symbol).filter(
                StockData.date == chosen_date
            ).distinct().order_by(StockData.symbol).all()

            companies = [row.symbol for row in result]
            logger.info(f"Retrieved {len(companies)} companies for {date}")
            return jsonify(companies)

        except ValueError:
            logger.error(f"Invalid date format: {date}")
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        except Exception as e:
            logger.error(f"Error in get_traded_companies: {e}")
            return jsonify({'error': str(e)}), 500

# Main Execution
if __name__ == "__main__":
    try:
        logger.info("Starting data processing pipeline")
        verify_tables()
        csv_path = r"C:\Users\nithe\Prabodh\Trading_Project\Combined NSE_Bhavcopy_Data"
        process_combined_csv(csv_path)
        aggregate_ohlc()
        calculate_pivot_points()
        logger.info("Data processing pipeline completed, starting Flask server")
        app.run(debug=True, host='0.0.0.0', port=5000)
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
