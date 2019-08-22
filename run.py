from redbeat.schedules import rrule
from redbeat import RedBeatSchedulerEntry
from test import test, app
from datetime import datetime, MINYEAR, timedelta

app.conf.redbeat_redis_url = 'redis://localhost'
print app
# print app.conf

'''
env BROKER_URL=redis://localhost python run.py
'''


def try_now():
    schedule = rrule('SECONDLY', interval=3, count=4)
    print(schedule) # <rrule: freq: 5, dtstart: 2017-12-09 20:17:03.882+00:00, interval: 3, count: 4, ...>
    entry = RedBeatSchedulerEntry('mee-wow-15', test.name, schedule, app=app)
    # entry.last_run_at = None
    # entry.last_run_at = datetime(MINYEAR, 1, 2)
    print schedule.dtstart
    print entry.last_run_at
    # print entry.is_due()
    # print entry.is_due()
    # print entry.is_due()
    # print entry.is_due()
    # print entry.is_due()
    entry.save()


def try_before():
    dt = datetime.utcnow() - timedelta(minutes=15)
    schedule = rrule('SECONDLY', dtstart=dt, interval=3, count=4)
    entry = RedBeatSchedulerEntry('before4', test.name, schedule, app=app)
    entry.last_run_at = None
    # print entry.is_due()
    # print entry.is_due()
    # print entry.is_due()
    # print entry.is_due()
    # print entry.is_due()

    entry.save()


if __name__ == '__main__':
    # try_now()
    try_before()
