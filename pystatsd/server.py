import functools
import re
import socket
import threading
import time
import types
import logging
import gmetric

log = logging.getLogger(__name__)

try:
    from setproctitle import setproctitle
except ImportError:
    setproctitle = None

from daemon import Daemon


__all__ = ['Server']


def _clean_key(k):
    return re.sub(
        r'[^a-zA-Z_\-0-9\.]',
        '',
        re.sub(
            r'\s+',
            '_',
            k.replace('/', '-').replace(' ', '_')
        )
    )

TIMER_MSG = '''%(prefix)s.%(key)s.lower %(min)s %(ts)s
%(prefix)s.%(key)s.count %(count)s %(ts)s
%(prefix)s.%(key)s.mean %(mean)s %(ts)s
%(prefix)s.%(key)s.upper %(max)s %(ts)s
%(prefix)s.%(key)s.upper_%(pct_threshold)s %(max_threshold)s %(ts)s
'''


def close_on_exn(fn):
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            fn(self, *args, **kwargs)
        except:
            self.stop()
            raise

    return wrapper


class Server(object):

    def __init__(self, pct_threshold=90, debug=False, transport='graphite',
                 ganglia_host='localhost', ganglia_port=8649,
                 ganglia_spoof_host='statsd:statsd', graphite_host='localhost',
                 graphite_port=2003, flush_interval=10,
                 no_aggregate_counters=False, counters_prefix='stats',
                 ganglia_counter_group='_counters',
                 timers_prefix='stats.timers', queue=None):
        self.running = True
        self._sock = None
        self._timer = None
        self._lock = threading.Lock()
        self.buf = 8192
        self.flush_interval = flush_interval
        self.pct_threshold = pct_threshold
        self.no_aggregate_counters = no_aggregate_counters

        if transport == 'ganglia':
            self.transport = TransportGanglia(
                # Ganglia specific settings
                host=ganglia_host,
                port=ganglia_port,
                protocol="udp",
                tmax=int(self.flush_interval),
                # Set DMAX to twice the flush interval. That should avoid
                # metrics to prematurely expire if there is some type of a
                # delay when flushing.
                dmax=int(self.flush_interval * 2 * 1000),
                # What hostname should these metrics be attached to.
                spoof_host=ganglia_spoof_host,
                counter_group=ganglia_counter_group,
                pct_threshold=pct_threshold,
                no_aggregate_counters=self.no_aggregate_counters,
            )
        elif transport == 'graphite':
            self.transport = TransportGraphite(
                # Graphite specific settings
                graphite_host=graphite_host,
                graphite_port=graphite_port,
                counters_prefix=counters_prefix,
                timers_prefix=timers_prefix,
                pct_threshold=pct_threshold,
            )
        elif transport == 'graphite_queue':
            self.transport = TransportGraphiteQueue(
                queue=queue,
                counters_prefix=counters_prefix,
                timers_prefix=timers_prefix,
                pct_threshold=pct_threshold,
            )
        else:
            self.transport = TransportNop()

        self.debug = debug
        self.counters = {}
        self.timers = {}
        self.flusher = 0

    def process(self, data):
        bits = data.split(':')
        key = _clean_key(bits[0])

        del bits[0]
        if len(bits) == 0:
            return

        self._lock.acquire()
        for bit in bits:
            sample_rate = 1
            fields = bit.split('|')
            if fields[1] is None:
                log.error('Bad line: %s' % bit)
                self._lock.release()
                return

            if (fields[1] == 'ms'):
                if key not in self.timers:
                    self.timers[key] = []
                self.timers[key].append(float(fields[0] or 0))
            else:
                if len(fields) == 3:
                    sample_rate = float(
                        re.match('^@([\d\.]+)', fields[2]).groups()[0])
                if key not in self.counters:
                    self.counters[key] = 0
                self.counters[key] += float(fields[0] or 1) * (1 / sample_rate)
        self._lock.release()

    @close_on_exn
    def flush(self):
        self._lock.acquire()
        ts = int(time.time())
        stats = 0

        self.transport.start_flush()

        for k, v in self.counters.items():
            v = float(v)
            if not self.no_aggregate_counters:
                v = v / self.flush_interval

            if self.debug:
                print "Sending %s => count=%s" % (k, v)

            self.transport.flush_counter(k, v, ts)

            self.counters[k] = 0
            stats += 1

        for k, v in self.timers.items():
            if len(v) > 0:
                # Sort all the received values. We need it to extract
                # percentiles.
                v.sort()
                count = len(v)
                min = v[0]
                max = v[-1]

                mean = min
                max_threshold = max

                if count > 1:
                    thresh_index = int((self.pct_threshold / 100.0) * count)
                    max_threshold = v[thresh_index - 1]
                    total = sum(v)
                    mean = total / count

                self.timers[k] = []

                if self.debug:
                    print "Sending %s ====> lower=%sms, mean=%sms, " \
                        "upper=%sms, %dpct=%sms, count=%s" % (
                            k, min, mean, max, self.pct_threshold,
                            max_threshold, count)

                self.transport.flush_timer(k, min, mean, max, count,
                                           max_threshold, ts)
                stats += 1

        self.transport.flush_statsd_stats(stats, ts)
        self.transport.finish_flush()
        self._lock.release()
        self._set_timer()

        if self.debug:
            print "\n================== Flush completed. Waiting until " \
                    "next flush. Sent out %d metrics =======" % (stats,)

    def _set_timer(self):
        if self.running:
            self._timer = threading.Timer(self.flush_interval, self.flush)
            self._timer.start()

    @close_on_exn
    def serve(self, hostname='', port=8125):
        assert type(port) is types.IntType, 'port is not an integer: %s' % (
            port,)
        addr = (hostname, port)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(addr)

        self._set_timer()
        while True:
            data, addr = self._sock.recvfrom(self.buf)
            self.process(data)

    def stop(self):
        # Have to running flag in case cancel is called while the timer is
        # being executed.
        log.info('Stopping...')
        self.running = False
        if self._timer is not None:
            self._timer.cancel()

        if self._sock is not None:
            try:
                # If you do not shutdown, the recvfrom call never returns.
                self._sock.shutdown(socket.SHUT_RD)
            except:
                pass

            self._sock.close()


class TransportGanglia(object):
    def __init__(self, host, port, protocol, tmax, dmax, spoof_host,
                 pct_threshold, counter_group, timing_group_prefix='',
                 no_aggregate_counters=False):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.tmax = tmax
        self.dmax = dmax
        self.spoof_host = spoof_host
        self.pct_threshold = pct_threshold
        self.counter_group = counter_group
        self.no_aggregate_counters = no_aggregate_counters
        self.g = None

    def start_flush(self):
        self.g = gmetric.Gmetric(self.host, self.port, self.protocol)

    def flush_counter(self, k, v, ts):
        self.g.send(k, v, "double", "count per sec", "both", self.tmax, self.dmax,
                    self.counter_group, self.spoof_host)

    def flush_timer(self, k, min_time, mean_time, max_time, count,
                    threshold_time, ts):
        # What group should these metrics be in. For the time being we'll set
        # it to the name of the key.
        group = k
        # Note we're converting the time units from ms to seconds so Ganglia
        # graphs look more sane.
        self.g.send(k + "_lower", float(min_time) / 1000.0, "double",
                    "seconds", "both", self.tmax, self.dmax, group, self.spoof_host)
        self.g.send(k + "_mean", float(mean_time) / 1000.0, "double",
                    "seconds", "both", self.tmax, self.dmax, group, self.spoof_host)
        self.g.send(k + "_upper", float(max_time) / 1000.0, "double",
                    "seconds", "both", self.tmax, self.dmax, group, self.spoof_host)
        self.g.send(k + "_" + str(self.pct_threshold) + "pct",
                    float(threshold_time) / 1000.0, "double", "seconds",
                    "both", self.tmax, self.dmax, group, self.spoof_host)
        self.g.send(k + "_count", count, "double", "count", "both",
                    self.tmax, self.dmax, group, self.spoof_host)

    def flush_statsd_stats(self, stats, ts):
        pass

    def finish_flush(self):
        self.g = None


class TransportGraphite(object):
    def __init__(self, host, port, counters_prefix, timers_prefix,
                 pct_threshold):
        self.host = host
        self.port = port
        if counters_prefix:
            counters_prefix = '%s.' % (counters_prefix,)
        self.counters_prefix = counters_prefix
        self.timers_prefix = timers_prefix
        self.pct_threshold = pct_threshold
        self.graphite_socket = None

    def start_flush(self):
        self.stat_string = ''

    def flush_counter(self, k, v, ts):
        msg = '%s%s %s %s\n' % (self.counters_prefix, k, v, ts)
        self.stat_string += msg

    def flush_timer(self, k, min, mean, max, count, max_threshold, ts):
        self.stat_string += TIMER_MSG % {
            'prefix': self.timers_prefix,
            'key': k,
            'mean': mean,
            'max': max,
            'min': min,
            'count': count,
            'max_threshold': max_threshold,
            'pct_threshold': self.pct_threshold,
            'ts': ts,
        }

    def flush_statsd_stats(self, stats, ts):
        self.flush_counter('statsd.numStats', stats, ts)
        graphite_socket = socket.socket()
        try:
            graphite_socket.connect((self.host, self.port))
            graphite_socket.sendall(self.stat_string)
            graphite_socket.close()
        except socket.error, e:
            log.error("Error communicating with Graphite: %s", e)
            if self.debug:
                print "Error communicating with Graphite: %s" % e

    def finish_flush(self):
        self.stat_string = ''


class TransportGraphiteQueue(object):
    def __init__(self, queue, counters_prefix, timers_prefix, pct_threshold):
        self.queue = queue
        if counters_prefix:
            counters_prefix = '%s.' % (counters_prefix,)
        self.counters_prefix = counters_prefix
        self.pct_threshold = pct_threshold
        self.timers_prefix = timers_prefix

    def start_flush(self):
        pass

    def flush_counter(self, k, v, ts):
        self.queue.put([
            ('%s%s' % (self.counters_prefix, k), (ts, v))
        ])

    def flush_timer(self, k, min_v, mean, max_v, count, max_threshold, ts):
        upper_n = 'upper_%s' % (self.pct_threshold,)
        self.queue.put([
            ('.'.join([self.timers_prefix, k, 'lower']), (ts, min_v)),
            ('.'.join([self.timers_prefix, k, 'count']), (ts, count)),
            ('.'.join([self.timers_prefix, k, 'mean']), (ts, mean)),
            ('.'.join([self.timers_prefix, k, 'upper']), (ts, max_v)),
            ('.'.join([self.timers_prefix, k, upper_n]), (ts, max_threshold)),
        ])

    def flush_statsd_stats(self, stats, ts):
        self.flush_counter('statsd.numStats', stats, ts)

    def finish_flush(self):
        pass


class TransportNop(object):
    def start_flush(self):
        pass

    def flush_counter(self, k, v, ts):
        pass

    def flush_timer(self, k, min, mean, max, count, max_threshold, ts):
        pass

    def flush_statsd_stats(self, stats, ts):
        pass

    def finish_flush(self):
        pass


class ServerDaemon(Daemon):
    def run(self, options):
        if setproctitle:
            setproctitle('pystatsd')
        server = Server(pct_threshold=options.pct,
                        debug=options.debug,
                        transport=options.transport,
                        graphite_host=options.graphite_host,
                        graphite_port=options.graphite_port,
                        ganglia_host=options.ganglia_host,
                        ganglia_spoof_host=options.ganglia_spoof_host,
                        ganglia_port=options.ganglia_port,
                        ganglia_counter_group=options.ganglia_counter_group,
                        flush_interval=options.flush_interval,
                        no_aggregate_counters=options.no_aggregate_counters,
                        counters_prefix=options.counters_prefix,
                        timers_prefix=options.timers_prefix)
        server.serve(options.name, options.port)


def run_server():
    import sys
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='debug mode', default=False)
    parser.add_argument('-n', '--name', dest='name',
                        help='bind listen socket to this hostname',
                        default='0.0.0.0')
    parser.add_argument('-p', '--port', dest='port',
                        help='port to run on', type=int,
                        default=8125)
    parser.add_argument('-r', '--transport', dest='transport',
                        help='transport to use graphite or ganglia', type=str,
                        default="graphite")
    parser.add_argument('--graphite-port', dest='graphite_port',
                        help='port to connect to graphite on',
                        type=int, default=2003)
    parser.add_argument('--graphite-host', dest='graphite_host',
                        help='host to which graphite metrics are sent',
                        type=str, default='localhost')
    parser.add_argument('--ganglia-port', dest='ganglia_port',
                        help='port to connect to ganglia on', type=int,
                        default=8649)
    parser.add_argument('--ganglia-host', dest='ganglia_host',
                        help='host to connect to ganglia on', type=str,
                        default='localhost')
    parser.add_argument('--ganglia-spoof-host', dest='ganglia_spoof_host',
                        help='host to report metrics as to ganglia', type=str,
                        default=None)
    parser.add_argument('--ganglia-counter-group',
                        help='the group to use for counter metrics', type=str,
                        # We put counters in _counters group. Underscore is to
                        # make sure counters show up first in the GUI.
                        default='_counters')
    parser.add_argument('--flush-interval', dest='flush_interval',
                        help='flush metrics every X seconds',
                        type=int, default=10)
    parser.add_argument('--no-aggregate-counters',
                        dest='no_aggregate_counters',
                        help='send raw counter values instead of count/sec',
                        action='store_true')
    parser.add_argument('--counters-prefix', dest='counters_prefix',
                        help='prepended to counter names for graphite',
                        type=str, default='statsd')
    parser.add_argument('--timers-prefix', dest='timers_prefix',
                        help='prepended to timing names for graphite',
                        type=str, default='statsd.timers')
    parser.add_argument('-t', '--pct', dest='pct',
                        help='percentile for timing data', type=int,
                        default=90)
    parser.add_argument('-D', '--daemon', dest='daemonize',
                        action='store_true', help='daemonize', default=False)
    parser.add_argument('--pidfile', dest='pidfile', action='store',
                        help='pid file',
                        default='/tmp/pystatsd.pid')
    parser.add_argument('--restart', dest='restart', action='store_true',
                        help='restart a running daemon', default=False)
    parser.add_argument('--stop', dest='stop', action='store_true',
                        help='stop a running daemon', default=False)
    options = parser.parse_args(sys.argv[1:])

    daemon = ServerDaemon(options.pidfile)
    if options.daemonize:
        daemon.start(options)
    elif options.restart:
        daemon.restart(options)
    elif options.stop:
        daemon.stop()
    else:
        daemon.run(options)

if __name__ == '__main__':
    run_server()
