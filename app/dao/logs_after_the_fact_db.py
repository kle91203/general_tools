import logging
import traceback

from app.dao import db_util, db

this_table = 'logs_after_the_fact'


def save_batch(rows):
    #################################### TODO if it fails, break the tuple in half and try each half. Keep doing that until you commit everything you can. Well, that might add a lot of time so maybe do that until you've gotten to the 8ths, then report back on the broken 8th (hopefully there's only one 8th that fails.)
    conn_with_active_transaction = db.get_conn_with_role(db_util.get_role_superuser())
    conn_with_active_transaction.set_session(autocommit=False)
    try:
        cur = conn_with_active_transaction.cursor()
        args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)", row).decode('utf-8') for row in rows)
        cur.execute('INSERT INTO ' + this_table + ' (time_stamp, whole_msg, time_stamp_2, level, msg) VALUES ' + args_str)
        conn_with_active_transaction.commit()
        conn_with_active_transaction.set_session(autocommit=True)
        rv = True
    except:
        rv = False
        logging.error('Caught exception. Rolling back.')
        logging.error(traceback.format_exc())
        try:
            conn_with_active_transaction.rollback()
        except:
            logging.error('Rollback failed')
        try:
            conn_with_active_transaction.set_session(autocommit=True)
        except:
            logging.error('Setting session back to autocommit failed')
    return rv

