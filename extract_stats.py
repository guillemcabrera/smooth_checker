import numpy as np
import csv
import sys

def print_statistics(data, field):
    print "{0} MIN:{1} MAX:{2} MEAN:{3} MED:{4} STD:{5} VAR:{6}".format(
            field,
            np.min(data[field]),
            np.max(data[field]),
            np.mean(data[field]),
            np.median(data[field]),
            np.std(data[field]),
            np.var(data[field]))


def read_csv(path):
    with open(path, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        rows = []

        # Calculate this fields from the given csv
        for row in reader:
            r = (float(row[1]), float(row[2]), int(row[3]),
                 float(row[3]) / float(row[2]) * 8 / 1000000,
                 float(row[2]) - float(row[1]))
            rows.append(r)
        return rows


def process_rows(rows):
    # Create a numpy array from the rows
    A = np.array(rows,
                 dtype=[('ttfb',float),('ttlb',float),('size',int),
                        ('bw',float),('dtime',float)])

    print_statistics(A, 'ttfb')
    print_statistics(A, 'ttlb')
    print_statistics(A, 'dtime')
    print_statistics(A, 'bw')


def usage():
    print "{0} <csv_file>".format(sys.argv[0])


if __name__ == "__main__":
    if len(sys.argv) == 1:
        usage()
        sys.exit(1)

    process_rows(read_csv(sys.argv[1]))
