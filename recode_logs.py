#!/usr/bin/env python3
import gzip
import json
from os import listdir
from sys import argv, stderr


def warn(msg):
    print(msg, file=stderr)


def log(msg, end='\n'):
    print(msg, end=end)


def extract_data(event_log):
    actor_urls = set()
    repo_urls = set()
    events = []
    num_errors = 0

    for event in event_log:

        try:
            actor, repo = event['actor']['url'], event['repo']['url']
            actor_urls.add(actor)
            repo_urls.add(repo)
            events.append((actor, repo))
        except Exception as e:
            num_errors += 1

    return (actor_urls, repo_urls, events, num_errors)


def convert_log_file(timestamp):
    log_filename = "{}.json.gz".format(timestamp)

    log("converting {} ... ".format(log_filename), end='')
    with gzip.open(log_filename, 'rt') as log_file:
        events = (json.loads(line) for line in log_file)
        actor_urls, repo_urls, events, num_errors = extract_data(events)

        if num_errors > 0:
            warn("lost {} events from {}".format(num_errors, log_filename))

        with gzip.open("{}-actors.json.gz".format(timestamp), 'wt') as actors_file:
            json.dump(list(actor_urls), actors_file)
        with gzip.open("{}-repos.json.gz".format(timestamp), 'wt') as repos_file:
            json.dump(list(repo_urls), repos_file)
        with gzip.open("{}-events.json.gz".format(timestamp), 'wt') as events_file:
            json.dump(events, events_file)
    log('done')

def find_logfile_timestamps(path):
    filenames = listdir(path)

    def times():
        for month in range(1, 12 + 1):
            for day in range(1, 31 + 1):
                for hour in range(0, 23 + 1):
                    yield (month, day, hour)

    for month, day, hour in times():
        timestamp = "2015-{:02}-{:02}-{}".format(month, day, hour)

        if "{}.json.gz".format(timestamp) in filenames:
            yield timestamp


if __name__ == "__main__":
    timestamps = argv[1:]

    if len(timestamps) == 0:
        print('no timestamps provided, assuming you want to convert all log\
         files in working directory.')

        timestamps = find_logfile_timestamps("./")

    for ts in timestamps:
        try:
            convert_log_file(ts)
        except Exception as e:
            warn("Error converting log file with timestamp = {}".format(ts))
            warn(e)
