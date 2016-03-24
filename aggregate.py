#!/usr/bin/env python3
import gzip
import json
from os import listdir
from sys import argv, stderr
from random import shuffle
from collections import OrderedDict


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

        def save(name, data):
            nonlocal timestamp
            with gzip.open("{}-{}.json.gz".format(timestamp, name), 'wt') as f:
                json.dump(data, f)

        save('actors', list(actor_urls))
        save('repos', list(repo_urls))
        save('events', events)

    log('done')


def times():
    for month in range(1, 12 + 1):
        for day in range(1, 31 + 1):
            for hour in range(0, 23 + 1):
                yield (month, day, hour)


def find_file_timestamps(path, ext='.json.gz'):
    filenames = listdir(path)

    for month, day, hour in times():
        timestamp = "2015-{:02}-{:02}-{}".format(month, day, hour)

        if "{}{}".format(timestamp, ext) in filenames:
            yield timestamp


def aggregate_set(path, typename):
    s, ext = set(), "{}.json.gz".format(typename)
    for ts in find_file_timestamps(path):
        s.update(json.load(gzip.open("{}-{}".format(ts, ext), "rt")))
    json.dump(list(s), gzip.open("all-{}".format(ext), "wt"))


def aggregate_repos(path):
    actors = list(json.load(gzip.open("all-actors.json.gz", "wt")))
    repos = set(json.load(gzip.open("all-repos.json.gz", "wt")))

    # derive an ordering for actors first come first serve
    shuffle(actors)
    deps = {actor: Counter() for actor in actors}

    for ts in find_file_timestamps():
        #  format = "YYYY-MM-DD-H"
        month = ts[5:7]
        day = ts[8:10]
        hour = ts[11:]

        events = json.load(gzip.open("{}-actors.json.gz".format(ts), "rt"))

        for actor, repo in events:
            deps[actor][repo] += 1

    urls = OrderedDict((actor, []) for actor in actors)
    for actor, repos in deps:
        if repo in repos:
            repos.remove(repo)
            urls[actor].add(repo)

    json.dump(list(actors), gzip.open("all-actors.json.gz", "wt"))


if __name__ == "__main__":
    aggregate_set('./', 'actors')
    aggregate_set('./', 'repos')
