import csv
import os
import gzip

from app.utils import super_special_magical_utils as ssmu, util


def write_to_csv(fieldnames, data, file_name):
    # TODO On the lambda, delete these once they're moved to S3
    outdir_base = '/tmp/wrench-exports/'
    timestamp = ssmu.super_magical_timestamp_maker()
    outdir = outdir_base + "/" + str(timestamp) + "/"
    util.ensure_folder(outdir)
    outfile = outdir + "/" + file_name  + '.csv'
    fh_csv_out = open(outfile, "w")
    csv_out = csv.DictWriter(fh_csv_out, fieldnames=fieldnames, extrasaction='ignore')
    csv_out.writeheader()
    csv_out.writerows(data)
    fh_csv_out.close()
    return outdir_base, timestamp, outfile


def gunzip(source_filepath, dest_filepath, block_size=65536):
    with gzip.open(source_filepath, 'rb') as s_file, \
            open(dest_filepath, 'wb') as d_file:
        while True:
            block = s_file.read(block_size)
            if not block:
                break
            else:
                d_file.write(block)
        d_file.write(block)


def files_in_dir(starting_dir_name, file_type):
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(starting_dir_name):
        for file in f:
            if file.endswith(file_type):
                files.append(os.path.join(r, file))
    return files
