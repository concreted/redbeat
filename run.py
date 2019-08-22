from redbeat.schedules import rrule
from redbeat import RedBeatSchedulerEntry
from test import test, app
from datetime import datetime, MINYEAR, timedelta
from time import sleep

app.conf.redbeat_redis_url = 'redis://localhost'
print app
# print app.conf

'''
env BROKER_URL=redis://localhost python run.py
'''

def test_entry(entry):
    while True:
        # print entry.is_due()
        result = entry.is_due()
        if result.is_due == True:
            print "meow"
            if result.next is None:
                break
        sleep(5)

def try_now():
    schedule = rrule('SECONDLY', interval=2, count=4)
    print(schedule) # <rrule: freq: 5, dtstart: 2017-12-09 20:17:03.882+00:00, interval: 3, count: 4, ...>
    entry = RedBeatSchedulerEntry('mee-wow-22', test.name, schedule, app=app)

    # test_entry(entry)

    entry.save()


def try_before():
    dt = datetime.utcnow() - timedelta(minutes=15)
    schedule = rrule('SECONDLY', dtstart=dt, interval=3, count=4)
    entry = RedBeatSchedulerEntry('before4', test.name, schedule, app=app)
    # entry.last_run_at = None
    test_entry(entry)

    # entry.save()


if __name__ == '__main__':
    try_now()
    # try_before()
