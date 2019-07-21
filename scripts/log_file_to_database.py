import logging
import os
import re
import traceback
import boto3

from botocore.config import Config

from app.dao import logs_after_the_fact_db, db
from app.utils import file_util


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


def break_msg_parts(msg):
    if msg:
        msg_parts = msg.split(' : ')
        if msg_parts:
            if len(msg_parts) == 3:
                return msg_parts
            elif len(msg_parts) < 3:
                pass
                #logging.warning('msg broke down into less than 3 parts.    msg:{}     msg_parts:{}'.format(msg, msg_parts))  ----- This is common. Don't log.
            else:
                logging.warning('msg broke down into more than 3 parts.    msg:{}     msg_parts:{}'.format(msg, msg_parts))
        else:
            logging.warning('msg_parts is falsy.    msg:{}     msg_parts:{}'.format(msg, msg_parts))
    else:
        logging.warning('msg is falsy. Unable to split it.    msg:{}'.format(msg))
    return None
    # p = re.compile('^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d') #Almost ISO8601
    # m = p.match(msg)
    # m2 = p.search(msg)


def to_batch_format(time_stamp, whole_msg, msg_parts):
    if not msg_parts:
        return (time_stamp, whole_msg, None, None, None)
    else:
        return (time_stamp, whole_msg, msg_parts[0], msg_parts[1], msg_parts[2])


def main():
    db.conn(user=os.getenv('DB_SUPERUSER_ACCOUNT_NAME'), pw=os.getenv('DB_SUPERUSER_PW'))
    db.conn(user=os.getenv('DB_CROSS_ACCOUNT_WRITER_NAME'), pw=os.getenv('DB_CROSS_ACCOUNT_WRITER_PW'))
    db.conn(user=os.getenv('DB_READ_ONLY_NAME'), pw=os.getenv('DB_READ_ONLY_PW'))

    # ---------------------- extract from .gz --------------------------------------------
    starting_dir = '/home/kevin/workspace/python/aws_tools/app/aws/dev/models/age'
    file_type = '.gz'
    files = file_util.files_in_dir(starting_dir, file_type)
    # for file in files:                                        |
    #     logging.info(file)                                    |
    #     filename = os.path.basename(file)                     |
    #     foldername = os.path.dirname(file)                    |---Turn this back on to unzip the S3 log files and extract the text file
    #     dest_filename = filename.replace('.gz', '.log')       |
    #     dest_path_and_file = foldername + '/' + dest_filename |
    #     file_util.gunzip(file, dest_path_and_file)            |

    #TODO This will reprocess every file. Make a table that will keep track of files that have already been processed
    file_type = '.log'
    files = file_util.files_in_dir(starting_dir, file_type)
    for file in files:
        logging.info('\n\n\n-----------------------------------------------------------------------------------------------------------------------------')
        logging.info('-----------------------------------------------------------------------------------------------------------------------------')
        logging.info('file: {}'.format(file))
        with open(file) as fp:
            line = fp.readline()
            cnt = 1
            db_batch = []
            while line:
                timestamp, msg, msg_parts = None, '', None
                if line and len(line) >= 24:
                    timestamp = line[:24]
                    if len(line) > 25:
                        msg = line[25:]
                        msg_parts = break_msg_parts(msg)
                if not timestamp:
                    logging.warning('Unrecognized format for line. Not saving. {}'.format(line))
                else:
                    tpl = to_batch_format(timestamp, msg, msg_parts)
                    db_batch.append(tpl)
                line = fp.readline()
                cnt += 1
                if cnt%10000 == 0:
                    logging.info(cnt)
                    #TODO capture the return (failed/succeeded) and deal with it
                    logs_after_the_fact_db.save_batch(db_batch)
                    db_batch = []



if __name__ == '__main__':
    main()
