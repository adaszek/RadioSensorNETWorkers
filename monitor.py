import time
import datetime
import redis
import math
import functools

from collections import defaultdict

def decode_capabilities(to_parse, sensor_list, array_of_cap):
    ret_dict = {}
    for it in to_parse:
        if it in sensor_list:
            s_ret_dict = {}
            reads, writes, reports = to_parse[it].split(";")
            rkey, rcaps = reads.split(":")
            r = list(map(lambda x: array_of_cap[x] if (x in array_of_cap) else None, rcaps.split(",")))
            s_ret_dict[rkey] = r
            wkey, wcaps = writes.split(":")
            w = list(map(lambda x: array_of_cap[x] if (x in array_of_cap) else None, wcaps.split(",")))
            s_ret_dict[wkey] = w
            pkey, prep = reports.split(":")
            s_ret_dict[pkey] = prep
            ret_dict[it] = s_ret_dict
        else:
             print("There is no such device as {}".format(it))
    return ret_dict

def monitor_sensors(pipe):
    sensors = pipe.smembers("sensors")
    functions = pipe.hgetall("sensors:functions")
    map_functions = pipe.hgetall("functions")
    decoded_sensors = decode_capabilities(functions, sensors, map_functions)

    all_last_activities = {}

    for sensor in sensors:
        keys_to_monitor = []
        for cap in decoded_sensors[sensor]["r"]:
            if cap is not None:
                keys_to_monitor.append("sensor:{sid}:{cid}:timestamps".format(sid=sensor, cid=cap))
        for cap in decoded_sensors[sensor]["w"]:
            if cap is not None:
                keys_to_monitor.append("sensor:{sid}:{cid}:timestamps".format(sid=sensor, cid=cap))

        sensorpipe = pipe.pipeline()

        while 1:
            try:
                sensorpipe.watch(keys_to_monitor)

                last_measurements = []

                sensorpipe.multi()
                for key in keys_to_monitor:
                    sensorpipe.zrange(key, -1, -1)
                    
                last_measurements = sensorpipe.execute()
                last_activity = int(functools.reduce(lambda x,y: x[0] if (x[0] > y[0]) else y[0], last_measurements))
                all_last_activities[sensor] = last_activity

                print("sid\t{sid}\tlast activity {past}\tago :: {act}".format(sid=sensor, past=(datetime.datetime.now() - datetime.datetime.fromtimestamp(last_activity)), act=time.ctime(last_activity)))
                break;
            except WatchError:
                continue

    pipe.multi()
    pipe.hmset("sensors:last_activity", all_last_activities)

def main():
    r = redis.StrictRedis(host='192.168.1.158', port=6379, db=0, encoding="utf-8", decode_responses=True)
    while 1:
        r.transaction(monitor_sensors, ["sensors", "sensors:functions", "functions"])
        time.sleep(5)

if __name__ == "__main__":
    main()
