#!/usr/bin/env python

import getopt
import sys
import os
import storage.storage as mod_storage

use_storage = False
storage_type = 'SQLite'
storage = None
test_mode = False
clear_old = False

try:
    optlist, args = getopt.getopt(sys.argv[1:], 'ctd:', ['clear-old', 'test', 'donor='])
    if '-d' in map(lambda item: item[0], optlist):
        donor = filter(lambda item: item[0]=='-d', optlist)[0][1]
    elif '--donor' in map(lambda item: item[0], optlist):
        donor = filter(lambda item: item[0]=='--donor', optlist)[0][1]
    else:
        raise Exception('donor site expected!')

    if '-t' in map(lambda item: item[0], optlist):
        test_mode = True
    elif '--test' in map(lambda item: item[0], optlist):
        test_mode = True
    else:
        test_mode = False

    if '-c' in map(lambda item: item[0], optlist):
        clear_old = True
    elif '--clear-old' in map(lambda item: item[0], optlist):
        clear_old = True
    else:
        clear_old = False

except Exception, ex:
    print '''
        Parser
    '''
    print 'Usage: %s [options]' % sys.argv[0]
    print '''
            Options:
                 -d <site-donor>  or --donor=<site-donor>  - Site for parsing. Required.
                 -t or --test Test mode. [Optional]
                 -c or --clear-old Delete old data from database. Optional
    '''
    print ex.message
    exit(1)

if use_storage:
    mod_dbase = __import__('storage.' + storage_type + '.crud', globals(), locals(), ['Crud'], -1)
    dbase = mod_dbase.Crud(donor)
    storage = mod_storage.Storage(dbase)
else:
    os.environ["SCRAPERWIKI_DATABASE_NAME"] = "sqlite:///db/" + donor + ".sqlite"

mod_scraper = __import__('donors.' + donor + '.scraper', globals(), locals(), ['Scraper'], -1)
scraper = mod_scraper.Scraper(storage)
if test_mode:
    scraper.test()
else:
    scraper.run(clear_old)


