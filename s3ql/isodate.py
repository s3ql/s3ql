#!/usr/bin/env python

"""
isodate.py

Functions for manipulating a subset of ISO8601 date, as specified by
  <http://www.w3.org/TR/NOTE-datetime>
  
Exposes:
  - parse(s)
    s being a conforming (regular or unicode) string. Raises ValueError for
    invalid strings. Returns a float (representing seconds from the epoch; 
    see the time module).
    
  - parse_datetime(s)   # if datetime module is available
    s being a conforming (regular or unicode) string. Raises ValueError for
    invalid strings. Returns a datetime instance.
    
  - asString(i)
    i being an integer or float. Returns a conforming string.
  
TOxDO:
  - Precision? it would be nice to have an interface that tells us how
    precise a datestring is, so that we don't make assumptions about it; 
    e.g., 2001 != 2001-01-01T00:00:00Z.

Thanks to Andrew Dalke for datetime support and Johan Herland for a patch.
"""

__license__ = """
Copyright (c) 2002-2009 Mark Nottingham <mnot@pobox.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Note: datetime support added by Andrew Dalke <dalke@dalkescientific.com>.
All copyrightable changes by Andrew Dalke are released into the public domain.  
No copyright protection is asserted.
"""


import sys, time, re, operator
from types import IntType, FloatType
from calendar import timegm

try:
    import datetime
except ImportError:
    _has_datetime = 0
else:
    _has_datetime = 1


__version__ = "0.8"

date_parser = re.compile(r"""^
    (?P<year>\d{4,4})
    (?:
        -
        (?P<month>\d{1,2})
        (?:
            -
            (?P<day>\d{1,2})
            (?:
                T
                (?P<hour>\d{1,2})
                :
                (?P<minute>\d{1,2})
                (?:
                    :
                    (?P<second>\d{1,2})
                    (?:
                        \.
                        (?P<dec_second>\d+)?
                    )?
                )?                    
                (?:
                    Z
                    |
                    (?:
                        (?P<tz_sign>[+-])
                        (?P<tz_hour>\d{1,2})
                        :
                        (?P<tz_min>\d{2,2})
                    )
                )?
            )?
        )?
    )?
$""", re.VERBOSE)


def parse(s):
    """ parse a string and return seconds since the epoch. """
    assert isinstance(s, basestring)
    r = date_parser.search(s)
    try:
        a = r.groupdict('0')
    except:
        raise ValueError, 'invalid date string format'
    d = timegm((   int(a['year']), 
                   int(a['month']) or 1, 
                   int(a['day']) or 1, 
                   int(a['hour']), 
                   int(a['minute']),
                   int(a['second']),
                   0,
                   0,
                   0
               ))
    return d - int("%s%s" % (
            a.get('tz_sign', '+'), 
            ( int(a.get('tz_hour', 0)) * 60 * 60 ) + \
            ( int(a.get('tz_min', 0)) * 60 ))
    )

if _has_datetime:
    def parse_datetime(s):
        """ parse a string and return a datetime object. """
        assert isinstance(s, basestring)
        r = date_parser.search(s)
        try:
            a = r.groupdict('0')
        except:
            raise ValueError, 'invalid date string format'

        dt = datetime.datetime(int(a['year']),
                               int(a['month']) or 1,
                               int(a['day']) or 1,
                               # If not given these will default to 00:00:00.0
                               int(a['hour']),
                               int(a['minute']),
                               int(a['second']),
                               # Convert into microseconds
                               int(float('0.' + a['dec_second'])*100000),
                               )
        tz_hours_offset = int(a['tz_hour'])
        tz_mins_offset = int(a['tz_min'])
        if a.get('tz_sign', '+') == "-":
            return dt + datetime.timedelta(hours = tz_hours_offset,
                                           minutes = tz_mins_offset)
        else:
            return dt - datetime.timedelta(hours = tz_hours_offset,
                                           minutes = tz_mins_offset)
    
def asString(i):
    """ given seconds since the epoch, return a dateTime string. """
    assert type(i) in [IntType, FloatType]
    year, month, day, hour, minute, second, wday, jday, dst = time.gmtime(i)
    o = str(year)
    if (month, day, hour, minute, second) == (1, 1, 0, 0, 0): return o
    o = o + '-%2.2d' % month
    if (day, hour, minute, second) == (1, 0, 0, 0): return o
    o = o + '-%2.2d' % day
    if (hour, minute, second) == (0, 0, 0): return o
    o = o + 'T%2.2d:%2.2d' % (hour, minute)
    if second != 0:
        o = o + ':%2.2d' % second
    o = o + 'Z'
    return o


def _cross_test():
    for iso in ("1997-07-16T19:20+01:00",
                "2001-12-15T22:43:46Z",
                "2004-09-26T21:10:15Z",
                "2004",
                "2005-04",
                "2005-04-30",
                "2004-09-26T21:10:15.1Z",
                "2004-09-26T21:10:15.1+05:00",
                "2004-09-26T21:10:15.1-05:00",
                ):
        timestamp = parse(iso)
        dt1 = datetime.datetime.utcfromtimestamp(timestamp)
        dt2 = parse_datetime(iso)
        if (dt1 != dt2 and
            dt1 != dt2.replace(microsecond=0)):
            raise AssertionError("Different: %r != %r" %
                                 (dt1, dt2))

if __name__ == "__main__":
    print parse("1997-07-16T19:20+01:00")
    print parse("2001-12-15T22:43:46Z")
    print parse("2004-09-26T21:10:15Z")
    if _has_datetime:
        _cross_test()
        print parse_datetime("1997-07-16T19:20+01:00")
        print parse_datetime("2001-12-15T22:43:46Z")
        print parse_datetime("2004-09-26T21:10:15Z")
