# Example: loops monitoring events forever.
# taken from https://github.com/seb-m/pyinotify/blob/master/python2/examples/loop.py
# this seems to do exactly what we need to with almost zero effort
# an
#
import pyinotify

# Instanciate a new WatchManager (will be used to store watches).
wm = pyinotify.WatchManager()
# Associate this WatchManager with a Notifier (will be used to report and
# process events).
notifier = pyinotify.Notifier(wm)
# Add a new watch on /tmp for ALL_EVENTS.
wm.add_watch('/tmp', pyinotify.ALL_EVENTS)
# Loop forever and handle events.
notifier.loop()