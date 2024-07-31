# Import the dependencies.
from flask import Flask, jsonify
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import pandas as pd
import datetime as dt


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# reflect an existing database into a new model
Base = automap_base()
Base.prepare(engine, reflect=True)
# reflect the tables


# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)


#################################################
# Flask Setup
#################################################
app = Flask(__name__)




#################################################
# Flask Routes
#################################################
@app.route('/')
def home():
    return (
        '''
        Welcome to the Climate API! Available routes:
        /api/v1.0/precipitation
        /api/v1.0/stations
        /api/v1.0/tobs
        /api/v1.0/<start>
        /api/v1.0/<start>/<end>
        '''
    )

@app.route('/api/v1.0/precipitation')
def precipitation():
    most_recent_date_str = session.query(func.max(Measurement.date)).scalar()
    most_recent_date = pd.to_datetime(most_recent_date_str)
    one_year_ago = most_recent_date - pd.DateOffset(years=1)
    
    query = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year_ago).all()
    precipitation_data = {date: prcp for date, prcp in query}
    return jsonify(precipitation_data)

@app.route('/api/v1.0/stations')
def stations():
    query = session.query(Station.station, Station.name).all()
    stations_list = [{'station': station, 'name': name} for station, name in query]
    return jsonify(stations_list)

@app.route('/api/v1.0/tobs')
def tobs():
    most_recent_date_str = session.query(func.max(Measurement.date)).scalar()
    most_recent_date = pd.to_datetime(most_recent_date_str)
    one_year_ago = most_recent_date - dt.timedelta(days=365)
    
    most_active_station_id = session.query(func.max(Measurement.station)).scalar()
    
    query = session.query(Measurement.tobs).filter(Measurement.station == most_active_station_id).filter(Measurement.date >= one_year_ago).all()
    tobs_list = [tobs for (tobs,) in query]
    return jsonify(tobs_list)

@app.route('/api/v1.0/<start>')
@app.route('/api/v1.0/<start>/<end>')
def temp_stats(start, end=None):
    if end:
        query = session.query(
            func.min(Measurement.tobs).label('min_temp'),
            func.avg(Measurement.tobs).label('avg_temp'),
            func.max(Measurement.tobs).label('max_temp')
        ).filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    else:
        query = session.query(
            func.min(Measurement.tobs).label('min_temp'),
            func.avg(Measurement.tobs).label('avg_temp'),
            func.max(Measurement.tobs).label('max_temp')
        ).filter(Measurement.date >= start).all()
    
    temp_stats = query[0]
    return jsonify({
        'TMIN': temp_stats.min_temp,
        'TAVG': temp_stats.avg_temp,
        'TMAX': temp_stats.max_temp
    })