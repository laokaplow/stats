#!/usr/bin/env python3
from subprocess import call


def times():
    for month in range(1, 12 + 1):
        for day in range(1, 31 + 1):
            for hour in range(0, 23 + 1):
                yield (month, day, hour)


def fetch_logfile(timestamps):
    template = "http://data.githubarchive.org/{}.json.gz"
    call(['wget', *[template.format(x) for x in timestamps]])


def fetch_all():
    fetch_logfile("2015-{:02}-{:02}-{}".format(*x) for x in times())


if __name__ == "__main__":
    fetch_all()
