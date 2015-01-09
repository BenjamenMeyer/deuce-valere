"""
Deuce Valere - API - System
"""
import contextlib
import datetime
import json

from deuceclient.api import Block
from stoplight import validate

from deucevalere.common.validation import *


class TimeManager(contextlib.ContextDecorator):

    def __init__(self, name):
        super().__init__()
        self.__name = name
        self.__start = None
        self.__end = None

    def serialize(self):
        def __serialize_time(t):
            if t is None:
                return 'none'
            else:
                return {
                    'year': t.year,
                    'month': t.month,
                    'day': t.day,
                    'hour': t.hour,
                    'minute': t.minute,
                    'second': t.second,
                    'microsecond': t.microsecond
                }

        return {
            'name': self.name,
            'start': __serialize_time(self.start),
            'end': __serialize_time(self.end)
        }

    @staticmethod
    def deserialize(serialized_data):
        def __deserialize_time(t):
            if isinstance(t, str):
                if t.lower() == 'none':
                    return None
                else:
                    raise ValueError('Unknown value')
            elif isinstance(t, dict):
                return datetime.datetime(
                    year=t['year'],
                    month=t['month'],
                    day=t['day'],
                    hour=t['hour'],
                    minute=t['minute'],
                    second=t['second'],
                    microsecond=t['microsecond'])
            else:
                raise TypeError('Unknown timestamp data type')

        tm = TimeManager(serialized_data['name'])
        tm.__start = __deserialize_time(serialized_data['start'])
        tm.__end = __deserialize_time(serialized_data['end'])
        return tm

    @property
    def name(self):
        return self.__name

    @property
    def start(self):
        return self.__start

    @property
    def end(self):
        return self.__end

    def reset(self):
        self.__start = None
        self.__end = None

    @property
    def elapsed(self):
        if self.start is None:
            return 0

        elif self.end is None:
            return TimeManager.__get_time() - self.start

        else:
            return self.end - self.start

    @staticmethod
    def __get_time():
        return datetime.datetime.utcnow()

    def __enter__(self):
        self.__start = TimeManager.__get_time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__end = TimeManager.__get_time()
        # We do not surpress anything here
        return False


class CounterManager(object):

    def __init__(self, name):
        self.__name = name
        self.__count = 0
        self.__byte_count = 0

    def serialize(self):
        return {
            'name': self.name,
            'count': self.count,
            'size': self.size
        }

    @staticmethod
    def deserialize(serialized_data):
        cm = CounterManager(serialized_data['name'])
        cm.add(serialized_data['count'],
               serialized_data['size'])
        return cm

    @property
    def name(self):
        return self.__name

    @property
    def count(self):
        return self.__count

    @property
    def size(self):
        return self.__byte_count

    def add(self, count, byte_count):
        self.__count = self.__count + count
        self.__byte_count = self.__byte_count + byte_count

    def reset(self):
        self.__count = 0
        self.__byte_count = 0


class ListManager(object):

    def __init__(self, name):
        self.__name = name
        self.__current = None
        self.__expired = None
        self.__deleted = None
        self.__orphaned = None

    def serialize(self):
        def __serialize_list(l):
            if l is None:
                return 'none'
            else:
                return l

        return {
            'name': self.name,
            'current': __serialize_list(self.current),
            'expired': __serialize_list(self.expired),
            'deleted': __serialize_list(self.deleted),
            'orphaned': __serialize_list(self.orphaned)
        }

    @staticmethod
    def deserialize(serialized_data):
        def __deserialize_list(l):
            if isinstance(l, str):
                if l.lower() == 'none':
                    return None
                else:
                    raise ValueError('Unknown value')
            elif (isinstance(l, list) or
                  isinstance(l, dict) or
                  isinstance(l, set)):
                return l
            else:
                raise TypeError('Unknown list data type')

        lm = ListManager(serialized_data['name'])
        lm.current = __deserialize_list(serialized_data['current'])
        lm.expired = __deserialize_list(serialized_data['expired'])
        lm.deleted = __deserialize_list(serialized_data['deleted'])
        lm.orphaned = __deserialize_list(serialized_data['orphaned'])
        return lm

    @property
    def name(self):
        return self.__name

    @property
    def current(self):
        return self.__current

    @current.setter
    def current(self, value):
        self.__current = value

    @property
    def expired(self):
        return self.__expired

    @expired.setter
    def expired(self, value):
        self.__expired = value

    @property
    def deleted(self):
        return self.__deleted

    @deleted.setter
    def deleted(self, value):
        self.__deleted = value

    @property
    def orphaned(self):
        return self.__orphaned

    @orphaned.setter
    def orphaned(self, value):
        self.__orphaned = value


class Manager(object):
    """
    Deuce Valere Manager
    """

    @validate(marker_start=MetadataBlockIdRuleNoneOkay,
              marker_end=MetadataBlockIdRuleNoneOkay,
              expire_age=ExpireAgeRuleNoneOkay)
    def __init__(self, marker_start=None, marker_end=None, expire_age=None):
        """
        :param marker_start: the start of the range to use, inclusive,
                             may be None
        :param marker_end: the end of the range to use, exclusive,
                           may be None
        """
        self.__times = {
            'validation': TimeManager('validation'),
            'cleanup': TimeManager('cleanup')
        }
        self.__counters = {
            'deleted': CounterManager('deleted'),
            'expired': CounterManager('expired'),
            'missing': CounterManager('missing'),
            'orphaned': CounterManager('orphaned')
        }
        self.__lists = {
            'metadata': ListManager('metadata'),
            'storage': ListManager('storage'),
            'xreference': {}
        }
        self.__markers = {
            'start': marker_start,
            'end': marker_end
        }

        if expire_age is None:
            dt_max = datetime.datetime.max
            dt_now = datetime.datetime.utcnow()
            expire_age = dt_max - dt_now

        self.__properties = {
            'expired_age': expire_age
        }

    def serialize(self):
        return {
            'expired_age': {
                'days': self.expire_age.days,
                'seconds': self.expire_age.seconds,
                'microseconds': self.expire_age.microseconds
            },
            'markers': {
                'start': self.__markers['start'],
                'end': self.__markers['end']
            },
            'times': {
                'validation': self.__times['validation'].serialize(),
                'cleanup': self.__times['cleanup'].serialize()
            },
            'counts': {
                'deleted': self.__counters['deleted'].serialize(),
                'expired': self.__counters['expired'].serialize(),
                'missing': self.__counters['missing'].serialize(),
                'orphaned': self.__counters['orphaned'].serialize()
            },
            'lists': {
                'metadata': self.__lists['metadata'].serialize(),
                'storage': self.__lists['storage'].serialize(),
                'xreference': self.__lists['xreference']
            }
        }

    def to_json(self):
        return json.dumps(self.serialize())

    @staticmethod
    def deserialize(serialized_data):
        exp = datetime.timedelta(
            days=serialized_data['expired_age']['days'],
            seconds=serialized_data['expired_age']['seconds'],
            microseconds=serialized_data['expired_age']['microseconds'])

        m = Manager(marker_start=serialized_data['markers']['start'],
                    marker_end=serialized_data['markers']['end'],
                    expire_age=exp)
        m.__times['validation'] = TimeManager.deserialize(
            serialized_data['times']['validation'])
        m.__times['cleanup'] = TimeManager.deserialize(
            serialized_data['times']['cleanup'])
        m.__counters['expired'] = CounterManager.deserialize(
            serialized_data['counts']['expired'])
        m.__counters['deleted'] = CounterManager.deserialize(
            serialized_data['counts']['deleted'])
        m.__counters['missing'] = CounterManager.deserialize(
            serialized_data['counts']['missing'])
        m.__counters['orphaned'] = CounterManager.deserialize(
            serialized_data['counts']['orphaned'])
        m.__lists['metadata'] = ListManager.deserialize(
            serialized_data['lists']['metadata'])
        m.__lists['storage'] = ListManager.deserialize(
            serialized_data['lists']['storage'])
        m.__lists['xreference'] = serialized_data['lists']['xreference']
        return m

    @staticmethod
    def from_json(json_data):
        serialized_data = json.loads(json_data)
        return Manager.deserialize(serialized_data)

    @property
    def expire_age(self):
        return self.__properties['expired_age']

    @expire_age.setter
    @validate(value=ExpireAgeRule)
    def expire_age(self, value):
        self.__properties['expired_age'] = value

    @property
    def start_block(self):
        return self.__markers['start']

    @property
    def end_block(self):
        return self.__markers['end']

    @property
    def validation_timer(self):
        return self.__times['validation']

    @property
    def cleanup_timer(self):
        return self.__times['cleanup']

    @property
    def deleted_counter(self):
        return self.__counters['deleted']

    @property
    def expired_counter(self):
        return self.__counters['expired']

    @property
    def missing_counter(self):
        return self.__counters['missing']

    @property
    def orphaned_counter(self):
        return self.__counters['orphaned']

    @property
    def metadata(self):
        return self.__lists['metadata']

    @property
    def storage(self):
        return self.__lists['storage']

    @property
    def cross_reference(self):
        return self.__lists['xreference']
