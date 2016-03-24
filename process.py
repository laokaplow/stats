#!/usr/bin/env python3

from collections import defaultdict, Counter
import gzip
import json


def dates():
    days_by_month = [
        31, 28, 31, 30,
        31, 30, 31, 31,
        30, 31, 30, 31
    ]

    for month, count in enumerate(days_by_month):
        for day in range(count):
            yield (month+1, day+1)


def hourly_timestamps(month, day):
    template = "2015-{:02}-{:02}-{}"
    return (template.format(month, day, hour) for hour in range(0, 24))


def download_log_files(month, day):
    timestamps = hourly_timestamps(month, day)
    url_template = "http://data.githubarchive.org/{}.json.gz"
    subprocess.call(['wget', *[url_template.format(ts) for ts in timestamps]])


def save(data, filename):
    with gzip.open("{}.json.gz".format(filename), "wt") as f:
        json.dump(data, f)


def log(msg): print(msg, end='', flush=True)


def recode_logs(month, day, path='./'):
    activity = defaultdict(Counter)
    for timestamp in hourly_timestamps(month, day):
        log('.')
        with gzip.open("{}{}.json.gz".format(path, timestamp), 'rt') as f:
            for event in (json.loads(line) for line in f):
                try:
                    actor = event['actor']['login']
                    repo = event['repo']['name']
                    activity[actor][repo] += 1
                except:
                    id = event['id']
                    raise RuntimeError("Error processing event #{}".format(id))

    save(activity, '2015-{:02}-{:02}-activity'.format(month, day))


def process_all(path):
    for month, day in dates():
        log("{:02}/{:02}:".format(month, day))
        try:
            # download_log_files(month, day)
            recode_logs(month, day, path)
        except Exception as e:
            log(e)
        log('\n')

if __name__ == "__main__":
    process_all("./")
