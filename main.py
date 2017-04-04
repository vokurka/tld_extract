import csv, sys, pprint, json, pip, os
pip.main(['install', 'tldextract'])
import tldextract

# IN = './in/'
# OUT = './out/'
IN = '/data/in/tables/'
OUT = '/data/out/tables/'

new_file_limit = 3500000
file_index = 0

if not os.path.exists(OUT + "tld_extracted.csv"):
    os.makedirs(OUT + "tld_extracted.csv")

manifest_data = {
    "source": "tld_extracted.csv",
    "destination": "in.c-ias.tld_extracted",
    "columns":
    ["pk", "dateReceived", "hitHour", "timeReceived", "host", "tld"]
}

manifest_f = open(
    OUT + "tld_extracted.csv.manifest", "w", encoding='utf-8', errors='ignore')
manifest_f.write(json.dumps(manifest_data))
manifest_f.close()

# Just open, do not read.
with open(
        IN + "logs_6th_Jan_sample.csv", "r", encoding='utf-8',
        errors='ignore') as inf:
    outf = open(
        OUT + "tld_extracted.csv/p_" + str(file_index),
        "w",
        encoding='utf-8',
        errors='ignore')
    writer = csv.writer(outf)
    reader = csv.reader(inf)

    counter = 0

    # Line by line => stream processing, nothing big in memory
    for row in reader:
        # 0       1           2          3         4
        #pk, dateReceived, hitHour, timeReceived, host

        ext = tldextract.extract(row[4])

        row.append(ext.domain + "." + ext.suffix)

        writer.writerow(row)

        if counter >= new_file_limit:
            counter = 0
            outf.close()
            file_index += 1
            outf = open(
                OUT + "tld_extracted.csv/p_" + str(file_index),
                "w",
                encoding='utf-8',
                errors='ignore')
            writer = csv.writer(outf)
