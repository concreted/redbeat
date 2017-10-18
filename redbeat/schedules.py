import celery
import pytz
from datetime import MINYEAR
from dateutil.rrule import (
    rrule as dateutil_rrule,
    YEARLY,
    MONTHLY,
    WEEKLY,
    DAILY,
    HOURLY,
    MINUTELY,
    SECONDLY
)
try:  # celery 4.x
    from celery.schedules import BaseSchedule as schedule
except ImportError:  # celery 3.x
    from celery.schedules import schedule

# from celery.utils.log import get_logger
# logger = get_logger(__name__)


class rrule(schedule):
    RRULE_REPR = (
        '<rrule: freq: {0.freq}, dtstart: {0.dtstart}, interval: {0.interval}, '
        'wkst: {0.wkst}, count: {0.count}, until: {0.until}, bysetpos: {0.bysetpos}, '
        'bymonth: {0.bymonth}, bymonthday: {0.bymonthday}, byyearday: {0.byyearday}, '
        'byeaster: {0.byeaster}, byweekno: {0.byweekno}, byweekday: {0.byweekday}, '
        'byhour: {0.byhour}, byminute: {0.byminute}, bysecond: {0.bysecond}, '
        'tzid: {0.tzid}>'
    )

    FREQ_MAP = {
        'YEARLY': YEARLY,
        'MONTHLY': MONTHLY,
        'WEEKLY': WEEKLY,
        'DAILY': DAILY,
        'HOURLY': HOURLY,
        'MINUTELY': MINUTELY,
        'SECONDLY': SECONDLY
    }

    def __init__(self, freq, dtstart=None,
                 interval=1, wkst=None, count=None, until=None, bysetpos=None,
                 bymonth=None, bymonthday=None, byyearday=None, byeaster=None,
                 byweekno=None, byweekday=None,
                 byhour=None, byminute=None, bysecond=None, tzid='UTC',
                 **kwargs):
        super(rrule, self).__init__(**kwargs)

        if type(freq) == str:
            freq_str = freq.upper()
            assert freq_str in rrule.FREQ_MAP
            freq = rrule.FREQ_MAP[freq_str]

        # Strip timezone data from dates. This is done because start/end times
        # are always assumed to be naive datetimes in the timezone defined by tzid.
        # Adjustments for DST are handled by normalizing the local time to UTC,
        # which accounts for variable offsets caused by DST.
        rrule_tz = pytz.timezone(tzid)
        if dtstart:
            dtstart = dtstart.replace(tzinfo=None)
        else:
            dtstart = pytz.utc.localize(self.now().replace(tzinfo=None)) \
                .astimezone(rrule_tz).replace(tzinfo=None)
        if until:
            until = until.replace(tzinfo=None)

        self.freq = freq
        self.dtstart = dtstart
        self.interval = interval
        self.wkst = wkst
        self.count = count
        self.until = until
        self.bysetpos = bysetpos
        self.bymonth = bymonth
        self.bymonthday = bymonthday
        self.byyearday = byyearday
        self.byeaster = byeaster
        self.byweekno = byweekno
        self.byweekday = byweekday
        self.byhour = byhour
        self.byminute = byminute
        self.bysecond = bysecond
        self.tzid = tzid
        self.rrule_tz = rrule_tz
        self.rrule = dateutil_rrule(freq, dtstart, interval, wkst, count, until,
                                    bysetpos, bymonth, bymonthday, byyearday, byeaster,
                                    byweekno, byweekday, byhour, byminute, bysecond)

    def remaining_estimate(self, last_run_at):
        # last_run_at is UTC. Convert to specified timezone for comparison with rrule.
        last_run_at_utc = pytz.utc.localize(last_run_at.replace(tzinfo=None))
        if last_run_at_utc.year == MINYEAR:
            # Redbeat uses a date with year == datetime.MINYEAR if the job was never run.
            # Since converting to local TZ may go out of range if this is the case,
            # don't convert it.
            last_run_at_local = last_run_at_utc
        else:
            last_run_at_local = last_run_at_utc.astimezone(self.rrule_tz)
        # Remove tzinfo from last_run_at_local
        # because rrule's dtstart does not have tzinfo.
        next_run = self.rrule.after(last_run_at_local.replace(tzinfo=None))
        if next_run:
            next_run_local = self.rrule_tz.localize(next_run)
            # Get UTC time of next run
            next_run_utc = pytz.utc.normalize(next_run_local)
            # Get delta between now and next run (using UTC times)
            now_utc = pytz.utc.localize(self.now().replace(tzinfo=None))
            delta = next_run_utc - now_utc
            return delta
        return None

    def is_due(self, last_run_at):
        rem_delta = self.remaining_estimate(last_run_at)
        if rem_delta is not None:
            rem = max(rem_delta.total_seconds(), 0)
            due = rem == 0
            if due:
                rem_delta = self.remaining_estimate(self.now())
                if rem_delta is not None:
                    rem = max(rem_delta.total_seconds(), 0)
                else:
                    rem = None
            return celery.schedules.schedstate(due, rem)
        return celery.schedules.schedstate(False, None)

    def __repr__(self):
        return self.RRULE_REPR.format(self)

    def __reduce__(self):
        return (self.__class__, (self.rrule), None)
