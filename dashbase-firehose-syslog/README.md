### Enable syslog to send to your syslog firehose:

#### On Max Osx:

* > cp /etc/syslog.conf /tmp/syslog.conf.bkp
* > edit /etc/syslog.conf
* > \*.\*                                       @192.168.1.12:port

Relaunch syslogd:

> $ sudo launchctl unload /System/Library/LaunchDaemons/com.apple.syslogd.plist
> $ sudo launchctl load /System/Library/LaunchDaemons/com.apple.syslogd.plist
