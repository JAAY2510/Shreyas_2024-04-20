from flask import Flask, jsonify, send_file 
import pandas as pd 
import sqlite3
import uuid 
import threading 
import os 
from datetime import datetime, timedelta
import pytz 
from typing import Dict, Tuple ,Optional 
import csv 
import io 

app = Flask(__name__)

DB_PATH = "store_monitoring.db"
REPORT_STATUS ={}
REPORT_OUTPUTS ={}

#initialize SQLite Database 

def init_db():
    conn = sqlite3.connect(DB_PATH)
    C = conn.cursor()

    C.execute(''' CREATE TABLE IF NOT EXISTS store_status( 
              store_id TEXT,
              timestamp_utc TEXT,
              status TEXT 
            )
              ''' )
    C.execute(''' CREATE TABLE IF NOT EXISTS business_hours( 
              store_id TEXT,
              day_of_week INTEGER,
              start_time_local TEXT,
              end_time_local  TEXT 
            )
              ''' )
    C.execute(''' CREATE TABLE IF NOT EXISTS timezones( 
              store_id TEXT,
              timezone_str TEXT
            )
              ''' )
    conn.commit()
    conn.close()

#csv to DB 
def load_csv_to_db():
    conn = sqlite3.connect(DB_PATH)
    #load_store_status 
    status_df = pd.read_csv("store_status.csv")
    status_df.to_sql('store_staus', conn , if_exists='replace', index = False)

    #Load_business_hours
    hours_df = pd.read_csv("menu_hours.csv")
    hours_df.to_sql('business_houurs',conn, if_exists = 'replace',index = False)

    #Load_timezones
    tz_df = pd.read_csv("timezones.csv")
    tz_df.to_sql('timezones', conn , if_exists = 'replace', index = False)

    conn.commit()
    conn.close()

# calculating up and down time of restraunts  

def get_store_timezone(store_id: str, conn) -> str:
    c = conn.cursor()
    c.execute('SELECT timezone_str FROM timezones WHERE store_id = ?', (store_id,)) 
    result = c.fetchone()
    return result[0] if result else 'America/Chicago'

def get_businees_hours(store_id: str, conn) -> Dict [int, Tuple [str,str]]:
    c = conn.cursor()
    c.execute(' SELECT day_of_week, start_time_local, end_time_local FROM businees_hours WHERE store_id =?', (store_id,))
    hours ={}
    for row in c.fetchall():
        hours[row[0]] = (row[1],row[2])
    if not hours: 
        for day in range(7):
            hours[day] = ("00:00:00", "23:59:59")
    return hours

def interpolate_status(statuses: list, interval_start: datetime, interval_end: datetime) -> Tuple[float, float]:

    total_minutes = (interval_end - interval_start).total_seconds() / 60
    if not statuses:
        return 0, total_minutes  # Assume down if no data
    uptime_minutes = 0
    current_time = interval_start
    for i, (timestamp, status) in enumerate(statuses):
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        if timestamp < interval_start:
            continue
        next_time = min(interval_end, timestamp) if i < len(statuses) - 1 else interval_end
        if current_time < next_time:
            minutes = (next_time - current_time).total_seconds() / 60
            if status == 'active' and i == 0:
                uptime_minutes += minutes
            elif status == 'active':
                # For subsequent points, use the status at the timestamp
                uptime_minutes += minutes       
        current_time = next_time
        if current_time >= interval_end:
            break
    downtime_minutes = total_minutes - uptime_minutes
    return uptime_minutes, downtime_minutes

def generate_report(report_id: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT MAX(timestamp_utc) FROM store_status')
    max_timestamp = datetime.fromisoformat(c.fetchone()[0].replace('Z', '+00:00'))
    output = io.StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow([
        'store_id',
        'uptime_last_hour',
        'uptime_last_day',
        'uptime_last_week',
        'downtime_last_hour',
        'downtime_last_day',
        'downtime_last_week'
    ])
    c.execute('SELECT DISTINCT store_id FROM store_status')
    store_ids = [row[0] for row in c.fetchall()]
    for store_id in store_ids:
        timezone_str = get_store_timezone(store_id, conn)
        tz = pytz.timezone(timezone_str)
        business_hours = get_businees_hours(store_id, conn)
        last_hour_end = max_timestamp
        last_hour_start = last_hour_end - timedelta(hours=1)
        last_day_start = last_hour_end - timedelta(days=1)
        last_week_start = last_hour_end - timedelta(days=7)
        c.execute('''
            SELECT timestamp_utc, status 
            FROM store_status 
            WHERE store_id = ? AND timestamp_utc >= ? 
            ORDER BY timestamp_utc
        ''', (store_id, last_week_start.isoformat()))
        statuses = [(datetime.fromisoformat(row[0].replace('Z', '+00:00')), row[1]) for row in c.fetchall()]
        uptime_last_hour, downtime_last_hour = 0, 0
        uptime_last_day, downtime_last_day = 0, 0
        uptime_last_week, downtime_last_week = 0, 0
        current_time = last_week_start
        while current_time < last_hour_end:
            day_end = min(current_time + timedelta(days=1), last_hour_end)
            day_of_week = current_time.weekday()
            if day_of_week in business_hours:
                start_time_str, end_time_str = business_hours[day_of_week]
                local_date = current_time.astimezone(tz).date()
                start_local = datetime.strptime(f"{local_date} {start_time_str}", "%Y-%m-%d %H:%M:%S")
                end_local = datetime.strptime(f"{local_date} {end_time_str}", "%Y-%m-%d %H:%M:%S")
                start_local = tz.localize(start_local)
                end_local = tz.localize(end_local)
                if end_local < start_local:
                    end_local += timedelta(days=1)
                interval_start = max(current_time, start_local.astimezone(pytz.UTC))
                interval_end = min(day_end, end_local.astimezone(pytz.UTC))
                if interval_start < interval_end:
                    # Filter relevant statuses
                    relevant_statuses = [
                        (t, s) for t, s in statuses 
                        if interval_start <= t <= interval_end
                    ]
                    uptime, downtime = interpolate_status(relevant_statuses, interval_start, interval_end)
                    if interval_end > last_hour_start:
                        uptime_last_hour += uptime
                        downtime_last_hour += downtime
                    if interval_end > last_day_start:
                        uptime_last_day += uptime / 60  # Convert to hours
                        downtime_last_day += downtime / 60
                    uptime_last_week += uptime / 60
                    downtime_last_week += downtime / 60
            current_time = day_end
        csv_writer.writerow([
            store_id,
            round(uptime_last_hour, 2),
            round(uptime_last_day, 2),
            round(uptime_last_week, 2),
            round(downtime_last_hour, 2),
            round(downtime_last_day, 2),
            round(downtime_last_week, 2)
        ])
    
    conn.close()
    REPORT_OUTPUTS[report_id] = output.getvalue()
    REPORT_STATUS[report_id] = 'Complete'
    output.close()

# implement API's 

@app.route('/trigger_report', methods =['POST'])
def trigger_report():
    report_id = str(uuid.uuid4())
    REPORT_STATUS[report_id] = 'Running'
    threading.Thread(target = generate_report, args = (report_id,)).start()
    return jsonify ({'report_id': report_id })

@app.route('/get_report/<report_id>', methods =['GET'])
def get_report(report_id):
    if report_id not in REPORT_STATUS:
        return jsonify({'error': 'Invalid report_id'}), 404
        
    if REPORT_STATUS[report_id] == 'Running':
        return jsonify({'status': 'Running'})
        
    # Generate CSV filesh
    output = io.StringIO(REPORT_OUTPUTS[report_id])
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode()),
        mimetype='text/csv',
        as_attachment=True,
        attachment_filename=f'report_{report_id}.csv'
    )


if __name__ == '__main__':
    init_db()
    load_csv_to_db()
    app.run(debug = True)
