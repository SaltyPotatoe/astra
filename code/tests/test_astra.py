import sys
import os
import time
from datetime import datetime
import pandas as pd
import pytest

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))
from src.astra import Astra  # noqa: E402

obs = None

########################

# startup (should be separated)
    # queue_get() thread
    # create_db()
    # __log()
    # read_config()
    # read_schedule()
    # load_devices()

def test_startup():
    '''
    Tests initialising Astra Object
    '''
    global obs 
    try:
        obs = Astra('/Users/peter/Github/astra/code/config/Callisto.yml')
        time.sleep(0.1) # to permit sqlworker to catchup
        ## TODO: status property denoting initialisation
        assert True
    except Exception as e:
        raise e

def test_queue_get():
    '''
    Queue thread test
    '''

    # find queue thread in obs.threads array
    queue_get_thread = None
    count = 0
    for i in obs.threads:
        if i['type'] == 'queue':
            count += 1
            queue_get_thread = i['thread']
    
    # check only one queue thread
    assert count == 1
    # check if alive
    assert queue_get_thread.is_alive() is True

def test_create_db():
    '''
    Test db creation
    '''

    rows = obs.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

    assert rows == [('polling',), ('images',), ('log',), ('autoguider_ref',), ('autoguider_log_new',), ('autoguider_info_log',), ('sqlite_sequence',)]

def test_log():
    '''
    Testing Astra's internal logger
    '''

    rows = obs.cursor.execute("SELECT * FROM log ORDER BY datetime DESC LIMIT 1;")

    dt = rows[0][0]
    # check last log in previous 1s
    assert (datetime.utcnow() - pd.to_datetime(dt)).total_seconds() < 1

    # check message is "Astra initialized"
    assert rows[0][2] == "Astra initialized"

def test_queue():
    '''
    Test queue after db creation
    '''

    obs.queue.put(({}, {"type" : "log", "data" : ('debug', 'Testing queue')}))
    
    start_time = time.time()
    while obs.queue.empty() is False:
        if time.time() - start_time > 1: # 1 seconds
            raise TimeoutError('queue_get timed out')
    
    # should return empty if queue_get thread is working
    assert obs.queue.empty() is True

def test_read_config():
    '''
    Tests reading the config file
    '''
    # TODO: more thorough test
    assert len(obs.observatory) > 0
    assert obs.error_free is True

def test_read_schedule():
    '''
    Tests reading the config file
    '''
    # TODO: more thorough test
    assert isinstance(obs.schedule, pd.DataFrame)
    assert obs.error_free is True

def test_load_devices():
    '''
    Tests loading devices
    '''
    # TODO: more thorough test
    assert isinstance(obs.devices, dict)
    assert obs.error_free is True

########################

# connect_all()
    # test device polling
    # start_watchdog() TODO: Move out of connect all

@pytest.mark.timeout(10)
def test_connect_all():
    '''
    Tests connect all.
    '''
    obs.connect_all()

    # TODO: more thorough test
    assert obs.error_free is True

def test_polling():
    '''
    Test that a device is begun polling
    '''

    polled_list = {}

    for device_type in obs.devices:
        
        polled_list[device_type] = {}

        for device_name in obs.devices[device_type]:
            
            polled_list[device_type][device_name] = {}

            polled = obs.devices[device_type][device_name].poll_latest()

            assert polled is not None

            polled_keys = polled.keys()

            assert len(polled_keys) > 0

            for k in polled_keys:

                polled_list[device_type][device_name][k] = {}
                polled_list[device_type][device_name][k]['value'] = polled[k]['value']
                polled_list[device_type][device_name][k]['datetime'] = polled[k]['datetime']

                assert (datetime.utcnow() - polled[k]['datetime']).total_seconds() < 5

def test_start_watchdog():
    '''
    Testing start_watchdog, which was started by connect_all.
    '''
    # find watchdog thread in obs.threads array
    watchdog_thread = None
    count = 0
    for i in obs.threads:
        if i['type'] == 'watchdog':
            count += 1
            watchdog_thread = i['thread']
    
    # check only one watchdog thread
    assert count == 1
    # check if alive
    assert watchdog_thread.is_alive() is True

########################

# def test_watchdog():
    # different scenerios

########################
    
# close_observatory()
    
def test_close_observatory():
    '''
    Test close observatory
    '''

    # close without paired_devices
    obs.close_observatory()

    # close with paired_devices
    paired_devices = obs.observatory['Camera'][0]['paired_devices']
    obs.close_observatory(paired_devices)

    # TODO: more thorough test
    assert obs.error_free is True

# open_observatory()
    
def test_open_observatory():
    '''
    Test open observatory
    '''

    # open without paired_devices
    obs.open_observatory()

    # close all to test with open paired devices
    obs.close_observatory()

    # open with paired_devices
    paired_devices = obs.observatory['Camera'][0]['paired_devices']
    obs.open_observatory(paired_devices)

    # TODO: more thorough test
    assert obs.error_free is True

# start_schedule()

def update_times(df, time_factor):
    '''
    Update the start and end times to present day factored by the time factor
    '''

    new_rows = []
    prev_start_time = None
    prev_end_time = None
    prev_new_start_time = None
    for i, row in df.iterrows():

        device_type, device_name, action_type, action_value, start_time, end_time, completed = row
        
        se_time_diff = end_time - start_time
        se_time_diff = se_time_diff / time_factor
        
        
        new_start_time = datetime.utcnow()
        
        
        if prev_end_time:
            ss_time_diff = start_time - prev_start_time
            ss_time_diff = ss_time_diff / time_factor
            
            new_start_time = prev_new_start_time + ss_time_diff
            
        
        new_end_time = new_start_time + se_time_diff

        new_row = [device_type, device_name, action_type, action_value, new_start_time, new_end_time, completed]
        new_rows.append(new_row)
        
        prev_start_time = start_time
        prev_end_time = end_time
        
        prev_new_start_time = new_start_time
    
    return pd.DataFrame(new_rows, columns=df.columns)

def write_schedule():
    import tempfile

    schedule = """
    device_type,device_name,action_type,action_value,start_time,end_time
    Camera,camera_Callisto,open,{},2024-01-11 23:31:40.915,2024-01-12 10:07:40.253
    Camera,camera_Callisto,flats,"{'filter': ['I+z'], 'n': [10]}",2024-01-11 23:31:40.915,2024-01-12 00:16:20.020
    Camera,camera_Callisto,object,"{'object': 'Sp0711-3824', 'filter': 'I+z', 'ra': 107.7545375, 'dec': -38.41298694444444, 'exptime': 13, 'guiding': True, 'pointing': False}",2024-01-12 00:16:20.020,2024-01-12 04:49:20.020
    Camera,camera_Callisto,object,"{'object': 'Sp0853-0329', 'filter': 'I+z', 'ra': 133.40066666666664, 'dec': -3.4922780555555555, 'exptime': 21, 'guiding': True, 'pointing': False}",2024-01-12 04:51:20.020,2024-01-12 09:23:00.030
    Camera,camera_Callisto,flats,"{'filter': ['I+z'], 'n': [10]}",2024-01-12 09:23:00.030,2024-01-12 10:07:40.253
    Camera,camera_Callisto,close,{},2024-01-12 10:07:40.253,2024-01-12 10:12:40.253
    Camera,camera_Callisto,calibration,"{'exptime': [0, 10, 13, 15, 21, 30, 60, 120], 'n': [10, 10, 10, 10, 10, 10, 10, 10]}",2024-01-12 10:12:40.253,2024-01-12 10:37:40.253
    """

    fp = tempfile.NamedTemporaryFile(mode='w+', delete=False)

    # write schedule to file
    fp.write(schedule)

    # Close the file
    fp.close()

    # df = pd.read_csv(fp.name)
    obs.schedule_path = fp.name

    obs.read_schedule()    
    obs.schedule = update_times(obs.schedule, 100)

def test_start_schedule():
    '''
    Test start schedule
    '''

    write_schedule()

    obs.start_schedule()

    obs.queue.put(({}, {"type" : "log", "data" : ('info', obs.schedule.to_string() )}))

    while obs.schedule_running is True:
        time.sleep(1)
        
        # if current time is greater than the end time of the last schedule
        if datetime.utcnow() > obs.schedule.iloc[-1]['end_time'] + pd.Timedelta(seconds=30):
            assert False

    assert obs.error_free is True
    assert obs.schedule_running is False
    



# cool_camera()

# pre_sequence()

# setup_observatory()

# object_sequence()

# flats_sequence()

# calibration_sequence()

# monitor_action()


# image saving, guiding?, should be in seperate tests.

