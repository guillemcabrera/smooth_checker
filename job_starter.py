#!/usr/bin/env python
#
# job_starter - RQ Smooth Streaming checking job starter
#
# Authored by Javier Lopez <jlopex[NO@SPAM]gmail.com> and
#             Guillem Cabrera <guillemcabrera[NO@SPAM]gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import csv
from redis import Redis
from rq import Queue
from optparse import OptionParser
from smoothchecker import check_media_job

__description = "Enqueue RQ tasks for Smooth Streaming checks"
__version = "0.1"
__author_info = "Javier Lopez and Guillem Cabrera"

JOB_TIMEOUT = 7200


def enqueue_medias_in_csv_file(csv_file):
    q = Queue(connection=Redis())
    with open(csv_file, 'rb') as csvfile:
        for row in csv.reader(csvfile, delimiter=','):
            q.enqueue(check_media_job,
                      _build_data(row[0], 'One', row[2], row[5]),
                      'results/', timeout=JOB_TIMEOUT)
            q.enqueue(check_media_job,
                      _build_data(row[1], 'Two', row[2], row[5]), 'results/',
                      timeout=JOB_TIMEOUT)
            return


def _build_data(url, cdn, media_key, streamable_id):
    return {
        'url': url,
        'cdn': cdn,
        'media_key': media_key,
        'streamable_id': streamable_id
    }


def options_parser():
    version = "%%prog %s" % __version
    usage = "usage: %prog [options] <manifest URL or file>"
    return OptionParser(usage=usage, version=version,
                        description=__description, epilog=__author_info)


if __name__ == "__main__":
    parser = options_parser()
    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        parser.exit(1)

    enqueue_medias_in_csv_file(args[0])
