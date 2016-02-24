# pg-wal-sequence

A python script that checks the validity of a WAL archive directory used 
for PostgreSQL PITR (Point In Time Recovery) purposes.

**Shell return codes (Nagios compatible)**
* Return code 0 for "OK"
* Return code 1 for "WARNING"
* Return code 2 for "CRITICAL"

### Usage from command line
```
$ chmod 755 pg-wal-sequence.py
$ ./pg-wal-sequence.py --directory /home/pitr/server1-wal
Missing WAL files at 000000010000050B000000FF (next WAL less than 1 day old)
$ echo $?
1
```

### Usage from Nagios
1. Copy the script to /usr/lib/nagios/plugins/ on some host running the Nagios agent (NRPE)
2. Open up /etc/nagios/nrpe.cfg
3. Add entry:
```
command[check_wal_sequence_server1]=/usr/lib/nagios/plugins/pg-wal-sequence.py \
                                    --directory/home/pitr/server1-wal
```
Now - the check_wal_sequence_server1 command has been installed and can be used from Nagios itself.
