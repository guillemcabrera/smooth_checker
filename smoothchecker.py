#!/usr/bin/env python
#
# smoothchecker - check Smooth Streaming chunk responses
# Based in the smooth-dl tool by Antonio Ospite <ospite@studenti.unina.it>
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

from csv import reader, writer
from glob import glob
from http.client import BadStatusLine
from multiprocessing import Pool, cpu_count
from optparse import OptionParser
from os import mkdir
from os.path import exists, join
from redis import Redis
from rq import get_current_job
from shutil import copyfileobj
from tempfile import gettempdir
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from xml.etree.ElementTree import parse

__description = 'Analyze Smooth Streaming stream chunks'
__version = '0.1'
__author_info = 'Javier Lopez and Guillem Cabrera'

RETRIES = 0
MS_BETWEEN_RETRIES = 1000


def get_manifest(base_url, dest_dir=gettempdir(), manifest_file='Manifest'):
    """
    Returns the manifest and the new URL if this is changed
    """
    if not exists(dest_dir):
        mkdir(dest_dir, mode=0o755)

    if base_url.startswith('http://'):
        manifest_url = base_url
        if not manifest_url.lower().endswith(('/manifest', '.ismc', '.csm')):
            manifest_url += '/Manifest'

        if base_url.lower().endswith('/manifest'):
            base_url = base_url[:base_url.rfind('/Manifest')]

        manifest_path = join(dest_dir, manifest_file)
        with open(manifest_path, 'wb') as f:
            f.write(urlopen(manifest_url).read())
    else:
        manifest_path = base_url

    _manifest = parse(manifest_path)
    if _manifest.getroot().attrib['MajorVersion'] != '2':
        raise Exception('Only Smooth Streaming version 2 supported')
    try:
        base_url = _manifest.find('Clip').attrib['Url'].lower()\
            .replace('/manifest', '')
    except Exception as _:
        pass

    return _manifest, base_url


def print_manifest_info(_manifest, _url):
    print(f"Manifest URL {_url}")
    for i, s in enumerate(_manifest.findall('.//StreamIndex')):
        stream_type = s.attrib['Type']
        print(f"Stream: {i} Type: {stream_type}")
        print('\tQuality Levels:')
        for j, q in enumerate(s.findall('QualityLevel')):
            bitrate = q.attrib['Bitrate']
            fourcc = q.attrib['FourCC']

            if stream_type == 'video':
                size = f"{q.attrib['MaxWidth']}x{q.attrib['MaxHeight']}"
                print(f"\t{j:2}: {fourcc:4} {size:10} @ {bitrate:7} bps")
            if stream_type == 'audio':
                channels = q.attrib['Channels']
                sampling_rate = q.attrib['SamplingRate']
                bits_per_sample = q.attrib['BitsPerSample']
                print(f"\t{j:2}: {fourcc:4} {sampling_rate}Hz "
                      f"{bits_per_sample}bits {channels}ch @ {bitrate:7} bps")
    print('')


def get_chunk_quality_string(stream, quality_level):
    quality = stream.findall('QualityLevel')[quality_level]
    quality_attributes = quality.findall('CustomAttributes/Attribute')
    custom_attributes = ''.join([f"{i.attrib['Name']}={i.attrib['Value']},"
                                 for i in quality_attributes]).rstrip(',')

    # Assume URLs are in this form:
    # Url="QualityLevels({bitrate})/Fragments(video={start time})"
    # or
    # Url="QualityLevels({bitrate},{CustomAttributes})/
    #     Fragments(video={start time})"
    return stream.attrib['Url'].split('/')[0]\
        .replace('{bitrate}', quality.attrib['Bitrate'])\
        .replace('{CustomAttributes}', custom_attributes)


def get_chunk_name_string(stream, chunk, i):
    try:
        t = chunk.attrib['t']
    except Exception:
        t = str(i)
    return stream.attrib['Url'].split('/')[1].replace('{start time}', t)


def check_medias_in_csv_file(csv_file, dest_dir):
    with open(csv_file, 'rb') as csvfile:
        for row in reader(csvfile, delimiter=','):
            _manifest, _url = get_manifest(row[0], dest_dir)
            print_manifest_info(_manifest)
            row.append(
                len(check_all_streams_and_qualities(_url, _manifest)) == 0)

            _manifest, _url = get_manifest(row[1], dest_dir)
            print_manifest_info(_manifest)
            row.append(
                len(check_all_streams_and_qualities(_url, _manifest)) == 0)

            with open(csv_file + '_out', 'ab') as f:
                writer(f).writerow(row)


def check_media_job(data, output_file_name):
    _manifest, _url = get_manifest(data['url'], gettempdir())
    print_manifest_info(_manifest, _url)
    errors = check_all_streams_and_qualities(_url, _manifest, 1)
    data['result'] = len(errors) < 1
    if not data['result']:
        data['errors'] = errors

    output_file_name = (f"{output_file_name}"
                        f"{get_current_job(connection=Redis()).id}.csv")
    with open(output_file_name, 'ab') as f:
        writer(f).writerow([data.get('url'),
                            data.get('cdn'),
                            data.get('media_key'),
                            data.get('streamable_id'),
                            data.get('result'),
                            data.get('errors')])
    return True


def check_all_streams_and_qualities(base_url, _manifest, processes):
    errors = []
    for i, s in enumerate(_manifest.findall('.//StreamIndex')):
        print(f"Checking stream {i}")
        for j, q in enumerate(s.findall('QualityLevel')):
            print(f"Checking quality {j}")
            errors.extend(check_chunks(base_url, _manifest, i, j, processes))
    return errors


def check_chunks(base_url, _manifest, stream_index, quality_level, processes):
    stream = _manifest.findall('.//StreamIndex')[stream_index]
    downloading_pool = Pool(processes=processes)
    results = []
    count = 0
    for c in stream.findall('c'):
        results.append(
            downloading_pool.apply_async(
                check_single_chunk,
                args=[base_url, get_chunk_quality_string(
                    stream, quality_level),
                      get_chunk_name_string(stream, c, count)]))
        count += int(c.attrib['d'])
    downloading_pool.close()
    downloading_pool.join()
    return [r.get() for r in results if r.get()[1] != 200]


def check_single_chunk(base_url, chunks_quality, chunk_name):
    chunk_url = base_url + '/' + chunks_quality + '/' + chunk_name
    try:
        response = _check_single_chunk(chunk_url)
        return chunk_url, response.getcode()
    except HTTPError as e:
        print(f"{e.url} returned {e.code}")
        return e.url, e.code
    except BadStatusLine:
        print(f"{chunk_url} returned bad status")
        return chunk_url, 0


def _check_single_chunk(chunk_url):
    request = Request(chunk_url)
    request.get_method = lambda: 'HEAD'
    return urlopen(request)


def results_join(output_file):
    with open(output_file, 'wb') as outfile:
        for result_file in glob('results/*.csv'):
            with open(result_file, 'rb') as readfile:
                copyfileobj(readfile, outfile)


def options_parser():
    _parser = OptionParser(
        usage="usage: %prog [options] <manifest URL or file>",
        version=("%%prog %s" % __version),
        epilog=__author_info)
    _parser.add_option('-i', '--info',
                       action='store_true', dest='info_only',
                       default=False, help='print Manifest info and exit')
    _parser.add_option('-m', '--manifest-only',
                       action='store_true', dest='manifest_only',
                       default=False, help='download Manifest file and exit')
    _parser.add_option('-r', '--prepare-results',
                       action='store_true', dest='results',
                       default=False, help='')
    _parser.add_option('-d', '--dest-dir', metavar='<dir>',
                       dest='dest_dir', default=gettempdir(),
                       help='destination directory')
    _parser.add_option('-p', '--parallel-processes', metavar='<int>',
                       dest='processes', default=cpu_count() * 2,
                       help='parallel processes to be launched')
    return _parser


if __name__ == '__main__':
    manifest, url = '', ''
    parser = options_parser()
    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        parser.exit(1)

    if args[0].startswith('http://'):
        url = args[0]
        manifest, url = get_manifest(url, options.dest_dir)
    elif options.results:
        results_join(args[0])
        parser.exit(0)
    else:
        check_medias_in_csv_file(args[0], options.dest_dir)
        parser.exit(0)

    if options.manifest_only:
        parser.exit(0)

    if options.info_only:
        print_manifest_info(manifest, url)
        parser.exit(0)

    print_manifest_info(manifest, url)
    check_all_streams_and_qualities(url, manifest, int(options.processes))
